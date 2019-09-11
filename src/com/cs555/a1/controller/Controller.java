package com.cs555.a1.controller;

import com.cs555.a1.Chunk;

import java.io.*;
import java.net.*;
import java.nio.channels.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Future;

import static java.util.Set.of;

public class Controller {
    static class ChunkMachine {
        int space;
        int numChunks;
        ChunkMachine(int space, int numChunks){
            this.space = space;
            this.numChunks = numChunks;
        }
    }

    private AsynchronousServerSocketChannel serverChannel;
    private Future<AsynchronousSocketChannel> acceptResult;
    private AsynchronousSocketChannel clientChannel;
    private ServerSocket ss;
    private boolean shutdown = false;
    private HashMap<String, Set<String>> chunkMap;  //chunks to machines which contain them
    private HashSet<String> chunkMachines = new HashSet<>();
    private HashMap<String, ChunkMachine> chunkMachineMap = null; //machines to metrice (free space and total number)
    
    public Controller(int controllerPort, String controllerMachine, int chunkPort, String[] chunkMachines) throws IOException {
        chunkMap = new HashMap<>();
        this.chunkMachines.addAll(Arrays.asList(chunkMachines));
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

    public void run() throws IOException, InterruptedException {
        // running infinite loop for getting
        // client request
        while (!shutdown)
        {
            Socket s = null;
            Thread.sleep(100);
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
    // ClientHandler class
    class ControllerClientHandler extends Thread
    {
        final DataInputStream in;
        final DataOutputStream out;
        final Socket s;

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
            while (true)
            {
                try {
                    String host = s.getInetAddress().getHostName();
                    //TODO this is weak logic, perhaps an (shared) enum or string verb to denote command types?
                    if (chunkMachines.contains(host)) {  //talking to a chunk server
                        processHeartbeat(host);
                    } else {  // talking to a client
                        processClientRequest();
                    }



                } catch (IOException e) {
                    e.printStackTrace();
                    break;
                }
            }

            try
            {
                this.in.close();
                this.out.close();
            } catch(IOException e){
                e.printStackTrace();
            }
        }

        private void processHeartbeat(String host) throws IOException {
            boolean isMajor = in.readBoolean();
            int freeSpace = in.readInt();
            int numChunks = in.readInt();
            HashSet<String> affirms = new HashSet<>();
            for (int i = 0; i < numChunks; i++) {
                int chunkVersion = in.readInt(); //currently unused
                String chunkName = in.readUTF();
                if (chunkMap.containsKey(chunkName)) {
                    chunkMap.get(chunkName).add(host);
                } else {
                    HashSet<String> machineSet = new HashSet<>();
                    machineSet.add(host);
                    chunkMap.put(chunkName, machineSet);
                }
                if (isMajor) {
                    affirms.add(chunkName);
                }
            }
            if (isMajor) {  // process deletions
                for (String chunkName : chunkMap.keySet()) {
                    // if global data says this host has it, but its latest major HB says it doesn't, delete
                    if (chunkMap.get(chunkName).contains(host) && !affirms.contains(chunkName)) {
                        chunkMap.get(chunkName).remove(host);
                    }
                }
            }
            //update machine info
            if (!chunkMachineMap.containsKey(host)) {
                chunkMachineMap.put(host, new ChunkMachine(freeSpace, numChunks));
            } else {
                chunkMachineMap.get(host).numChunks = numChunks;
                chunkMachineMap.get(host).space = freeSpace;
            }
        }

        private void processClientRequest() {
            //figure out if it's a read or write

        }
    }
}
