package com.cs555.a1.controller;

import com.cs555.a1.Helper;

import java.io.*;
import java.net.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.cs555.a1.Helper.replicationFactor;

public class Controller {

    static class ChunkMachine implements Comparable<ChunkMachine> {
        String name;
        int freeSpace;
        int numChunks;
        ChunkMachine(String name, int space, int numChunks){
            this.name = name;
            this.freeSpace = space;
            this.numChunks = numChunks;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }

            if (!ChunkMachine.class.isAssignableFrom(obj.getClass())) {
                return false;
            }

            final ChunkMachine other = (ChunkMachine) obj;
            return Objects.equals(this.name, other.name);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public int compareTo(ChunkMachine c) {
            return this.freeSpace - c.freeSpace; // descending order (biggest space first)
        }

        @Override
        public String toString() {
            return String.format("name: %s, freeSpace: %s, numChunks: %s", name, freeSpace, numChunks);
        }
    }

    private ServerSocket ss;
    private boolean shutdown = false;
    private final ConcurrentHashMap<String, HashSet<String>> chunksToMachines;  //chunks to machines which contain them
    private final ArrayList<ChunkMachine> chunkMachines; //machines to metrics (free space and total number)
    private int chunkPort;

    public Controller(int controllerPort, int chunkPort) throws IOException {
        this.chunksToMachines = new ConcurrentHashMap<>();
        this.chunkMachines = new ArrayList<>();
        this.chunkPort = chunkPort;
        ss = new ServerSocket(controllerPort);
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                Close();
                mainThread.join();
            } catch (InterruptedException | IOException e) {
                System.exit(-1);
            }
        }));
    }

    private void Close() throws IOException {
        ss.close();
        shutdown = true;
    }

    public void run() throws IOException {
        // running infinite loop for getting
        // client request
        Thread mT = new ControllerChunkWatcher();
        mT.start();
        while (!shutdown)
        {
            Socket s = null;
            try
            {
                s = ss.accept();
                DataInputStream in = new DataInputStream(s.getInputStream());
                DataOutputStream out = new DataOutputStream(s.getOutputStream());
                Thread t = new ControllerRequestHandler(s, in, out);
                t.start();
            }
            catch (Exception e){
                if (s != null){
                    s.close();
                }
                e.printStackTrace();
            }
        }
    }

    private class ControllerChunkWatcher extends Thread {

        @Override
        public void run() {
            Instant start = Instant.now();
            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    System.out.println("Heartbeat thread interrupted, stopping");
                    return;
                }
                if (Duration.between(start, Instant.now()).toSeconds() > Helper.MinorHeartbeatSeconds) {
                    start = Instant.now();
                    synchronized (chunkMachines) {
                        synchronized (chunksToMachines) {
                            Iterator<ChunkMachine> chunkMachineIterator = chunkMachines.iterator();
                            while (chunkMachineIterator.hasNext()) {
                                ChunkMachine chunkMachine = chunkMachineIterator.next();
                                try (
                                        Socket s = new Socket(chunkMachine.name, chunkPort);
                                        DataOutputStream out = new DataOutputStream(s.getOutputStream());
                                        DataInputStream in = new DataInputStream(s.getInputStream())
                                ) {
                                    out.writeUTF("heartbeat");
                                    if (Helper.debug)
                                        System.out.println("Sending heartbeat to chunk server " + chunkMachine.name);
                                    if (!in.readBoolean()) {
                                        throw new IOException();
                                    }
                                } catch (IOException e) {
                                    System.out.println("Chunk server failure detected: " + chunkMachine.name);
                                    chunkMachineIterator.remove();
                                    handleFailedServer(chunkMachine);
                                }
                            }
                        }

                    }
                }
            }
        }

        private void handleFailedServer(ChunkMachine chunkMachine) {
            //look through all chunks we know about, remove this server from their set of servers
            for (String chunk : chunksToMachines.keySet()) {
                // usually this means do nothing, since not all servers maintain all chunks
                boolean contained = chunksToMachines.get(chunk).remove(chunkMachine.name);
                //if this means nobody has it, remove the chunk completely
                if (chunksToMachines.get(chunk).isEmpty()) {
                    chunksToMachines.remove(chunk);
                } else if (contained) {
                    // the chunk was at this server, now we need to forward it to other machines
                    HashSet<String> servers = new HashSet<>(chunksToMachines.get(chunk));
                    //store a server which has the chunk
                    Iterator<String> serverIterator = servers.iterator();
                    String first = serverIterator.next();
                    ArrayList<String> serverList = new ArrayList<>();
                    serverList.add(first);
                    // add servers which don't contain the chunk (so not already in the set)
                    for (int i = 0; serverList.size() + servers.size() - 1 < replicationFactor
                            && i < chunkMachines.size(); i++) {
                        String candidate = chunkMachines.get(chunkMachines.size() - 1).name;
                        if (!servers.contains(candidate))
                            serverList.add(candidate);
                    }
                    if (serverList.size() > 1) {
                        // send this on to the chunk server(s)--they will forward the file if they have it
                        System.out.println("Reinforcing replication on " + chunk + " via " + serverList);
                        Helper.writeToChunkServerWithForward(new byte[0], chunk, serverList, chunkPort);
                    }
                }
            }
        }
    }

    // ClientHandler class
    class ControllerRequestHandler extends Thread
    {
        final DataInputStream in;
        final DataOutputStream out;
        final Socket s;
        private Random rng = new Random();

        // Constructor
        ControllerRequestHandler(Socket s, DataInputStream in, DataOutputStream out)
        {
            this.s = s;
            this.in = in;
            this.out = out;
        }

        @Override
        public void run()
        {
            try {
                String host = s.getInetAddress().getHostName();
                switch (in.readUTF()) {
                    case "write":
                        handleWrite();
                        break;
                    case "read":
                        handleRead(host);
                        break;
                    case "heartbeat":
                        processHeartbeat(host, in);
                        System.out.println("heartbeat processed, chunk machines: " + chunkMachines.toString());
                        break;
                    case "taddle":
                        removeChunk(host);
                        break;
                    default:
                        //out.writeUTF("Invalid input");
                        break;
                }
                this.in.close();
                this.out.close();
                this.s.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void removeChunk(String host) throws IOException {
            String fileName = in.readUTF();
            int numMachines = in.readInt();
            ArrayList<String> machines = new ArrayList<>();
            for (int i = 0; i < numMachines; i++)
                machines.add(in.readUTF());
            if (numMachines == 0)
                machines.add(host);
            System.out.println("Taddle: Removing file " + fileName + " from machines " + machines);
            synchronized (chunkMachines) {
                synchronized (chunksToMachines) {
                    for (String machine : machines) {
                        HashSet<String> machineSet = chunksToMachines.get(fileName);
                        if (machineSet != null && machineSet.remove(machine)) {
                            for (ChunkMachine cm : chunkMachines) {
                                if (cm.name.equals(machine)) {
                                    cm.freeSpace++;
                                    cm.numChunks--;
                                }
                            }
                        }
                    }
                    chunkMachines.sort(ChunkMachine::compareTo);
                }
            }
        }

        private void handleRead(String host) throws IOException {
            String fileName;
            fileName = in.readUTF();
            boolean isFailure = in.readBoolean();
            if (chunksToMachines.containsKey(fileName)) {
                HashSet<String> machines = chunksToMachines.get(fileName);
                if (isFailure) { //isFailure
                    machines.remove(host);
                }
                HashSet<String> readMatches = new HashSet<>(machines);
                if (in.readBoolean() && !isFailure) //is chunk server?
                    readMatches.remove(host);
                if (readMatches.isEmpty()) {
                    out.writeBoolean(false);
                } else {
                    out.writeBoolean(true);
                    String randServer = getRandomElement(readMatches, rng);
                    out.writeUTF(randServer);
                }
            } else {
                out.writeBoolean(false);
            }
        }

        private void handleWrite() throws IOException {
            String fileName;
            synchronized (chunkMachines) {
                synchronized (chunksToMachines) {
                    fileName = in.readUTF();
                    // we need up-to-date sorting (from heartbeats)
                    if (chunkMachines.isEmpty()) {
                        out.writeBoolean(false);
                    } else if (chunkMachines.get(chunkMachines.size() - 1).freeSpace <= 0) {
                        out.writeBoolean(false);
                    } else {
                        out.writeBoolean(true);
                        HashSet<String> writeMachines = chunksToMachines.computeIfAbsent(fileName, k -> new HashSet<>());
                        int replicationFactor = Integer.min(Helper.replicationFactor, chunkMachines.size());
                        // send to servers which have it first
                        // then to servers which have the most room (up to replication factor)
                        out.writeInt(replicationFactor);
                        for (String machine : writeMachines) {
                            out.writeUTF(machine);
                        }
                        int numSent = writeMachines.size();
                        for (int i = 0; numSent < replicationFactor; i++) {
                            String bestServer = chunkMachines.get(chunkMachines.size() - 1 - i).name;
                            if (!writeMachines.contains(bestServer)) {
                                out.writeUTF(bestServer);
                                decrementSpace(bestServer);
                                writeMachines.add(bestServer);
                                numSent++;
                            }
                        }
                        chunkMachines.sort(ChunkMachine::compareTo);
                    }
                }
            }
        }

        private void decrementSpace(String chunkServer) {
            for (ChunkMachine m : chunkMachines) {
                if (m.name.equals(chunkServer)) {
                    m.numChunks++;
                    m.freeSpace--;
                    return;
                }
            }
        }

        private String getRandomElement(HashSet<String> readMatches, Random rng) {
            int randInt = rng.nextInt(readMatches.size());
            int i = 0;
            for (String machine : readMatches) {
                if (i == randInt)
                    return machine;
                i++;
            }
            return "";
        }

        private void processHeartbeat(String host, DataInputStream in) throws IOException {
            synchronized (chunkMachines) {
                synchronized (chunksToMachines) {
                    boolean isMajor = in.readBoolean();
                    int numChunks = in.readInt();
                    int numMsgChunks = in.readInt();
                    int freeSpace = Helper.space - numChunks;
                    HashSet<String> affirms = new HashSet<>();
                    for (int i = 0; i < numMsgChunks; i++) {
                        int chunkVersion = in.readInt(); //currently unused
                        String chunkName = in.readUTF();
                        if (chunksToMachines.containsKey(chunkName)) {
                            chunksToMachines.get(chunkName).add(host);
                        } else {
                            HashSet<String> machineSet = new HashSet<>();
                            machineSet.add(host);
                            chunksToMachines.put(chunkName, machineSet);
                        }
                        if (isMajor) {
                            affirms.add(chunkName);
                        }
                    }
                    if (isMajor) {  // process deletions
                        for (String chunkName : chunksToMachines.keySet()) {
                            // if global data says this host has it, but its latest major HB says it doesn't, delete
                            if (chunksToMachines.get(chunkName).contains(host) && !affirms.contains(chunkName)) {
                                chunksToMachines.get(chunkName).remove(host);
                            }
                        }
                    }
                    // update chunk machine metadata (primarily for serving write requests)
                    boolean found = false;
                    boolean needsSort = true;
                    for (ChunkMachine cm : chunkMachines) {
                        if (cm.name.equals(host)) {
                            cm.numChunks = numChunks;
                            if (cm.freeSpace == freeSpace)
                                needsSort = false;
                            cm.freeSpace = freeSpace;
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        //System.out.println("Adding new chunk machine at " + host);
                        chunkMachines.add(new ChunkMachine(host, freeSpace, numChunks));
                    }
                    if (!needsSort)
                        chunkMachines.sort(ChunkMachine::compareTo);
                }
            }
        }
    }
}
