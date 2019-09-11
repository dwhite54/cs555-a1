package com.cs555.a1.chunkserver;

import com.cs555.a1.Chunk;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.concurrent.Future;

public class ChunkServer {
    private int controllerPort;
    private String controllerMachine;
    private int chunkPort;
    private String[] chunkMachines;
    private AsynchronousServerSocketChannel serverChannel;
    private Future<AsynchronousSocketChannel> acceptResult;
    private AsynchronousSocketChannel clientChannel;
    private ServerSocket ss;
    private boolean shutdown = false;
    private Instant start = Instant.now();
    //need to store the chunks that are at this server (filename with underscore and integer appended),
    //for each chunk we need version, and 8 SHA-1 hashes (1 per 8KB),
    private HashMap<String, Chunk> chunks;
    private int space = 100;

    public ChunkServer(int controllerPort, String controllerMachine, int chunkPort, String[] chunkMachines) throws IOException {
        this.controllerPort = controllerPort;
        this.controllerMachine = controllerMachine;
        this.chunkPort = chunkPort;
        this.chunkMachines = chunkMachines;
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
            long minsElapsed = Duration.between(start, Instant.now()).toSeconds();
            try
            {
                if (minsElapsed % 300 == 0){
                    //send major heartbeat
                    Thread MT = new ChunkControllerHandler(true);
                    MT.start();
                } else if (minsElapsed % 30 == 0) {
                    //send minor heartbeat
                    Thread mT = new ChunkControllerHandler(false);
                    mT.start();
                }

                //TODO heartbeats are the only affirmation of chunk storage, should we force a heartbeat upon successful write?

                // socket object to receive incoming client requests
                s = ss.accept();

                System.out.println("A new client is connected : " + s);

                // obtaining input and out streams
                DataInputStream dis = new DataInputStream(s.getInputStream());
                DataOutputStream dos = new DataOutputStream(s.getOutputStream());

                System.out.println("Assigning new thread for this client");

                // create a new thread object
                Thread t = new ChunkClientHandler(dis, dos);

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

    //from https://stackoverflow.com/questions/4895523/java-string-to-sha1
    private static String hexify(byte[] b) {
        StringBuilder result = new StringBuilder();
        for (byte value : b) {
            result.append(Integer.toString((value & 0xff) + 0x100, 16).substring(1));
        }
        return result.toString();
    }

    //from https://stackoverflow.com/questions/4895523/java-string-to-sha1
    private byte[] getSHA1(byte[] chunk) throws NoSuchAlgorithmException {
        MessageDigest crypt = MessageDigest.getInstance("SHA-1");
        crypt.reset();
        crypt.update(chunk);
        return crypt.digest();
    }

    private String getSHA1Hex(byte[] chunk) throws NoSuchAlgorithmException {
        return hexify(getSHA1(chunk));
    }

    private class ChunkControllerHandler extends Thread {
        private boolean isMajor;
        ChunkControllerHandler(boolean isMajor) {
            this.isMajor = isMajor;
        }

        @Override
        public void run() {
            try {
                Socket s = new Socket(controllerMachine, controllerPort);
                DataOutputStream out = new DataOutputStream(s.getOutputStream());
                out.writeBoolean(isMajor);
                out.writeInt(space);
                int numChunks = 0;
                if (isMajor)
                    numChunks = chunks.size();
                else {  // find out how many we will write--this is ugly and could be improved! RACE CONDITION??
                    for (String key : chunks.keySet()) {
                        if (chunks.get(key).isNew) {
                            numChunks++;
                        }
                    }
                }
                out.writeInt(numChunks);
                for (String key : chunks.keySet()) {
                    Chunk chunk = chunks.get(key);
                    if (chunk.isNew || isMajor) {
                        out.writeUTF(chunk.filename);
                        out.writeInt(chunk.version);
                    }
                }
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
                dumpStack();
            }
        }
    }

    // ClientHandler class
    class ChunkClientHandler extends Thread
    {
        private final DataInputStream in;
        private final DataOutputStream out;

        // Constructor
        ChunkClientHandler(DataInputStream in, DataOutputStream out)
        {
            this.in = in;
            this.out = out;
        }

        @Override
        public void run()
        {
            String received;
            String toreturn;
            while (true)
            {
                try {
                    received = in.readUTF();
                    String[] split = received.split(" ");
                    switch (split[0]) {
                        case "write" :
                            toreturn = "List of available online servers with space.";
                            break;
                        case "read" :
                            toreturn = "Chunk server which contains this chunk";
                            break;
                        case "hminor" :
                            toreturn = "nothing";
                            break;
                        case "hmajor" :
                            toreturn = "nothing";
                            break;
                        default:
                            toreturn = "Invalid input";
                            break;
                    }
                    out.writeUTF(toreturn);
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
    }
}
