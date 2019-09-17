package com.cs555.a1.chunkserver;

import com.cs555.a1.Chunk;
import com.cs555.a1.Helper;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class ChunkServer {
    private int controllerPort;
    private String controllerMachine;
    private int chunkPort;
    private ServerSocket ss;
    private boolean shutdown = false;
    //need to store the chunks that are at this server (filename with underscore and integer appended),
    //for each chunk we need version, and 8 SHA-1 hashes (1 per 8KB),
    private HashMap<String, Chunk> chunks;

    public ChunkServer(int controllerPort, String controllerMachine, int chunkPort) throws IOException {
        this.controllerPort = controllerPort;
        this.controllerMachine = controllerMachine;
        this.chunkPort = chunkPort;
        this.chunks = new HashMap<>();
        ss = new ServerSocket(chunkPort);
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
        Thread mT = new ChunkControllerHandler();
        mT.start();
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
    private byte[] getSHA1(byte[] chunk) throws NoSuchAlgorithmException {
        MessageDigest crypt = MessageDigest.getInstance("SHA-1");
        crypt.reset();
        crypt.update(chunk);
        return crypt.digest();
    }

    private class ChunkControllerHandler extends Thread {
        @Override
        public void run() {
            Instant start = Instant.now();
            int numMinor = 0;
            boolean isMajor = false;
            while (true) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    System.out.println("Heartbeat thread interrupted, stopping");
                    return;
                }
                if (Duration.between(start, Instant.now()).toSeconds() > Helper.MinorHeartbeatSeconds) {
                    start = Instant.now();
                    if (numMinor == Helper.MajorHeartbeatSeconds / Helper.MinorHeartbeatSeconds) isMajor = true;
                    System.out.println("Sending heartbeat to controller, ismajor = " + Boolean.toString(isMajor));
                    try (
                            Socket s = new Socket(controllerMachine, controllerPort);
                            DataOutputStream out = new DataOutputStream(s.getOutputStream())
                    ) {
                        out.writeUTF("heartbeat");
                        out.writeBoolean(isMajor);
                        out.writeInt(Helper.space);
                        out.writeInt(chunks.size());
                        int numChunks = chunks.size();
                        if (!isMajor) {  // find out how many we will write--this is ugly and could be improved! RACE CONDITION??
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
                                chunk.isNew = false;
                                out.writeUTF(chunk.fileName);
                                out.writeInt(chunk.version);
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                        dumpStack();
                    } finally {
                        isMajor = false;
                        numMinor++;
                    }
                }
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
            try {
                String fileName;
                int fileSize;
                byte[] fileContents;
                switch (in.readUTF()) {
                    case "write" :
                        fileName = in.readUTF();
                        fileSize = in.readInt();  // likely 64k, but check anyway (could be last chunk)
                        fileContents = new byte[fileSize];
                        in.readFully(fileContents);
                        int numForwards = in.readInt();
                        ArrayList<String> forwards = new ArrayList<>();
                        for (int i = 0; i < numForwards; i++)
                            forwards.add(in.readUTF());
                        boolean isForwarded = Helper.writeToChunkServerWithForward(fileContents, fileName, forwards, chunkPort);
                        boolean isWritten = writeChunk(fileName, fileContents);
                        out.writeBoolean(isForwarded && isWritten);
                        break;
                    case "read" :
                        fileName = in.readUTF();
                        if (chunks.containsKey(fileName)) {
                            fileContents = readChunk(fileName);
                            if (fileContents == null) {
                                String readServer = Helper.readFromController(controllerMachine, controllerPort, fileName);
                                fileContents = Helper.readFromChunkServer(fileName, readServer, chunkPort);
                            }
                            out.writeInt(fileContents.length);
                            out.write(fileContents);
                        } else {
                            out.writeInt(0);
                        }
                        break;
                    case "heartbeat" :  // tell the controller we are still here
                        System.out.println("Responding to controller heartbeat");
                        out.writeBoolean(true);
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

        private byte[] readChunk(String fileName) {
            try {
                String fullPath = Paths.get(Helper.chunkHome, fileName).toString();
                FileInputStream fileInputStream = new FileInputStream(fullPath);
                byte[] contents = fileInputStream.readAllBytes();

                if (!validateChunk(fileName, contents)) return null;

                return contents;
            } catch (IOException | NoSuchAlgorithmException e) {
                e.printStackTrace();
                return null;
            }
        }

        private boolean validateChunk(String fileName, byte[] contents) throws IOException, NoSuchAlgorithmException {
            FileInputStream hashInputStream = new FileInputStream(fileName + ".sha1");
            byte[] hashes = hashInputStream.readAllBytes();

            //check if the number of slices equals the number of hashes
            int numSlices = contents.length / Helper.BpSlice;
            int sliceRemainder = contents.length % Helper.BpSlice;
            if (sliceRemainder > 0) {  // if we have extra data beyond the last full 8KB slice, consider it a new slice
                numSlices++;
            }

            if (numSlices > Helper.slicesPerChunk) {
                return false;
            }

            int numHashes = hashes.length / Helper.BpHash;
            int hashRemainder = hashes.length % Helper.BpHash;
            if (hashRemainder > 0) {
                return false;
            }

            if (numHashes != numSlices) {
                return false;
            }

            for (int i = 0; i < numSlices; i++) {
                byte[] oldHash = Arrays.copyOfRange(hashes, i*Helper.BpHash, (i*Helper.BpHash)+Helper.BpHash);
                byte[] slice = Arrays.copyOfRange(contents, i*Helper.BpSlice, (i*Helper.BpSlice)+Helper.BpSlice);
                byte[] newHash = getSHA1(slice);
                if (!Arrays.equals(oldHash, newHash)) {
                    return false;
                }
            }
            return true;
        }

        private boolean writeChunk(String fileName, byte[] contents) {
            try {
                File file = new File(Paths.get(Helper.chunkHome, fileName).toString());
                if (file.exists()) {
                    Chunk chunk = chunks.get(fileName);
                    chunk.version++;
                    chunk.isNew = true;
                } else {
                    Chunk chunk = new Chunk();
                    chunk.fileName = fileName;
                    chunk.version = 1;
                    String[] splits = fileName.split("_");
                    chunk.sequence = Integer.parseInt(splits[splits.length - 1]);
                    chunks.put(fileName, chunk);
                }

                int numSlices = contents.length / Helper.BpSlice;
                if (contents.length % Helper.BpSlice > 0) {
                    numSlices++;
                }

                byte[] hashes = new byte[numSlices * Helper.BpHash];
                for (int i = 0; i < numSlices; i++) {
                    byte[] hash = getSHA1(Arrays.copyOfRange(contents, i * Helper.BpSlice, (i * Helper.BpSlice) + Helper.BpSlice));
                    System.arraycopy(hash, 0, hashes, (i * Helper.BpHash), hash.length);
                }

                FileOutputStream hashStream = new FileOutputStream(file.getPath() + ".sha1");
                hashStream.write(hashes);
                return true;
            } catch (NoSuchAlgorithmException | IOException e) {
                e.printStackTrace();
                return false;
            }
        }
    }
}
