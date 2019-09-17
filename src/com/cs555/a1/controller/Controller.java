package com.cs555.a1.controller;

import com.cs555.a1.Helper;

import java.io.*;
import java.net.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class Controller {

    static class ChunkMachine implements Comparable<ChunkMachine> {
        String name;
        int freeSpace;
        int numChunks;
        HashSet<String> chunks;
        ChunkMachine(String name, int space, int numChunks){
            this.name = name;
            this.freeSpace = space;
            this.numChunks = numChunks;
            this.chunks = new HashSet<>();
        }

        @Override
        public int compareTo(ChunkMachine c) {
            return this.freeSpace - c.freeSpace; // descending order (biggest space first)
        }
    }

    private ServerSocket ss;
    private boolean shutdown = false;
    private HashMap<String, HashSet<String>> chunksToMachines;  //chunks to machines which contain them
    private TreeSet<ChunkMachine> chunkMachines; //machines to metrics (free space and total number)
    private int chunkPort;
    
    public Controller(int controllerPort, int chunkPort) throws IOException {
        this.chunksToMachines = new HashMap<>();
        this.chunkMachines = new TreeSet<>();
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
        Thread mT = new ControllerChunkHandler();
        mT.start();
        while (!shutdown)
        {
            Socket s = null;
            try
            {

                // socket object to receive incoming client requests
                s = ss.accept();

                System.out.println("A new client is connected : " + s);

                // obtaining input and out streams
                DataInputStream in = new DataInputStream(s.getInputStream());
                DataOutputStream out = new DataOutputStream(s.getOutputStream());

                System.out.println("Assigning new thread for this client");

                // create a new thread object
                Thread t = new ControllerClientHandler(s, in, out);

                // Invoking the start() method
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

    private class ControllerChunkHandler extends Thread {

        @Override
        public void run() {
            Instant start = Instant.now();
            while (true) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    System.out.println("Heartbeat thread interrupted, stopping");
                    return;
                }
                if (Duration.between(start, Instant.now()).toSeconds() > Helper.MinorHeartbeatSeconds) {
                    start = Instant.now();
                    for (ChunkMachine chunkMachine : chunkMachines) {
                        try (
                                Socket s = new Socket(chunkMachine.name, chunkPort);
                                DataOutputStream out = new DataOutputStream(s.getOutputStream());
                                DataInputStream in = new DataInputStream(s.getInputStream())
                        ) {
                            out.writeUTF("heartbeat");
                            System.out.println("Sending heartbeat to chunk server");
                            if (!in.readBoolean())
                                chunkMachines.remove(chunkMachine);
                        } catch (IOException e) {
                            chunkMachines.remove(chunkMachine);
                        }
                    }
                }
            }
        }
    }

    // ClientHandler class
    class ControllerClientHandler extends Thread
    {
        final DataInputStream in;
        final DataOutputStream out;
        final Socket s;
        private Random rng = new Random();

        // Constructor
        ControllerClientHandler(Socket s, DataInputStream in, DataOutputStream out)
        {
            this.s = s;
            this.in = in;
            this.out = out;
        }

        @Override
        public void run()
        {
            try {
                String fileName;
                String host = s.getInetAddress().getHostName();
                switch (in.readUTF()) {
                    case "write" :
                        fileName = in.readUTF();
                        ChunkMachine candidate = chunkMachines.last(); // largest freeSpace
                        HashSet<String> writeMachines = chunksToMachines.get(fileName);
                        if (writeMachines.isEmpty()) {
                            int numServers = Integer.min(Helper.replicationFactor, chunkMachines.size());
                            out.writeInt(numServers);
                            for (int i = 0; i < numServers; i++) {
                                assert candidate != null;
                                out.writeUTF(candidate.name);
                                candidate = chunkMachines.lower(candidate);
                            }
                        } else {
                            out.writeInt(writeMachines.size());
                            assert writeMachines.size() >= Helper.replicationFactor;  // else replication has failed
                            for (String machine : writeMachines) {
                                out.writeUTF(machine);
                            }
                        }
                        break;
                    case "read" :
                        fileName = in.readUTF();
                        if (chunksToMachines.containsKey(fileName)) {
                            out.writeBoolean(true);
                            HashSet<String> readMatches = chunksToMachines.get(fileName);
                            out.writeUTF(getRandomElement(readMatches, rng));
                        } else {
                            out.writeBoolean(false);
                        }
                        break;
                    case "heartbeat" :
                        System.out.println("processing heartbeat from " + host);
                        processHeartbeat(host, in);
                        break;
                    default:
                        out.writeUTF("Invalid input");
                        break;
                }
                this.in.close();
                this.out.close();
            } catch (IOException e) {
                e.printStackTrace();
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
            boolean isMajor = in.readBoolean();
            int freeSpace = in.readInt();
            int numChunks = in.readInt();
            int numMsgChunks = in.readInt();
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
            for (ChunkMachine cm : chunkMachines) {
                if (cm.name.equals(host)) {
                    chunkMachines.remove(cm);
                    cm.numChunks = numChunks;
                    cm.freeSpace = freeSpace;
                    chunkMachines.add(cm);
                    found = true;
                }
            }
            if (!found) {
                chunkMachines.add(new ChunkMachine(host, freeSpace, numChunks));
            }
        }
    }
}
