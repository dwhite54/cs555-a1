package com.cs555.a1.controller;

import java.io.*;
import java.net.*;
import java.nio.channels.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.Future;

import static java.util.Set.of;

public class Controller {
    static class ChunkMachine {
        int space;
        int numChunks;
        HashSet<String> chunks;
        ChunkMachine(int space, int numChunks){
            this.space = space;
            this.numChunks = numChunks;
            this.chunks = new HashSet<>();
        }
    }

    private AsynchronousServerSocketChannel serverChannel;
    private Future<AsynchronousSocketChannel> acceptResult;
    private AsynchronousSocketChannel clientChannel;
    private ServerSocket ss;
    private boolean shutdown = false;
    private HashSet<String> machines;
    private HashMap<String, HashSet<String>> chunksToMachines;  //chunks to machines which contain them
    private HashMap<String, ChunkMachine> machinesToMetrics = null; //machines to metrics (free space and total number)
    
    public Controller(int controllerPort, String controllerMachine, int chunkPort, String[] chunkMachines) throws IOException {
        chunksToMachines = new HashMap<>();
        this.machines.addAll(Arrays.asList(chunkMachines));
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
            try {
                String fileName;
                String host = s.getInetAddress().getHostName();
                switch (in.readUTF()) {
                    case "write" :
                        fileName = in.readUTF();
                        if (chunksToMachines.containsKey(fileName)) {
                            out.writeBoolean(true);
                            HashSet<String> matchedMachines = chunksToMachines.get(fileName);
                            //todo get top 3 machines by space
                        } else {
                            out.writeBoolean(false);
                        }
                        break;
                    case "read" :

                        break;
                    case "heartbeat" :
                        processHeartbeat(host);
                        break;
                    default:
                        out.writeUTF("Invalid input");
                        break;
                }
            } catch (IOException e) {
                e.printStackTrace();
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
            //update space and total chunks per machine
            if (!machinesToMetrics.containsKey(host)) {
                machinesToMetrics.put(host, new ChunkMachine(freeSpace, numChunks));
            } else {
                machinesToMetrics.get(host).numChunks = numChunks;
                machinesToMetrics.get(host).space = freeSpace;
            }
        }
    }
}
