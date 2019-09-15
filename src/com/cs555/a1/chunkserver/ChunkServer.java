package com.cs555.a1.chunkserver;

import com.cs555.a1.Chunk;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.Future;

public class ChunkServer {
    private String chunkHome = "/tmp/dwhite54/chunks";
    private int BpSlice = 1024*8;
    private int slicesPerFile = 8;
    private int BpHash = 20;
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
        this.chunks = new HashMap<>();
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
                        boolean result = writeChunk(fileName, fileContents);
                        out.writeBoolean(result);
                        break;
                    case "read" :
                        fileName = in.readUTF();
                        if (chunks.containsKey(fileName)) {
                            fileContents = readChunk(fileName);
                            if (fileContents != null) {
                                out.writeInt(fileContents.length);
                                out.write(fileContents);
                            } else {
                                //TODO handle failure by 1) tell controller, getting alt CS 2) get alt CS file 3) send to client
                            }
                        } else if (new File(fileName).exists()){
                            // TODO handle when the file exists but our in-memory metadata says it doesn't (server crashed)
                        } else {
                            out.writeInt(0);
                        }
                        break;
                    case "heartbeat" :  // tell the controller we are still here
                        out.writeBoolean(true); //TODO send false when the chunk server is undergoing repair?
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

        private byte[] readChunk(String fileName) {
            try {
                String fullPath = Paths.get(chunkHome, fileName).toString();
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
            int numSlices = contents.length / BpSlice;
            int sliceRemainder = contents.length % BpSlice;
            if (sliceRemainder > 0) {  // if we have extra data beyond the last full 8KB slice, consider it a new slice
                numSlices++;
            }

            if (numSlices > slicesPerFile) {
                return true;
            }

            int numHashes = hashes.length / BpHash;
            int hashRemainder = hashes.length % BpHash;
            if (hashRemainder > 0) {
                return true;
            }

            if (numHashes != numSlices) {
                return true;
            }

            for (int i = 0; i < numSlices; i++) {
                byte[] oldHash = Arrays.copyOfRange(hashes, i*BpHash, (i*BpHash)+BpHash);
                byte[] slice = Arrays.copyOfRange(contents, i*BpSlice, (i*BpSlice)+BpSlice);
                byte[] newHash = getSHA1(slice);
                if (!Arrays.equals(oldHash, newHash)) {
                    return true;
                }
            }
            return false;
        }

        private boolean writeChunk(String fileName, byte[] contents) {
            try {
                File file = new File(Paths.get(chunkHome, fileName).toString());
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

                int numSlices = contents.length / BpSlice;
                if (contents.length % BpSlice > 0) {
                    numSlices++;
                }

                byte[] hashes = new byte[numSlices * BpHash];
                for (int i = 0; i < numSlices; i++) {
                    byte[] hash = getSHA1(Arrays.copyOfRange(contents, i * BpSlice, (i * BpSlice) + BpSlice));
                    System.arraycopy(hash, 0, hashes, (i * BpHash), hash.length);
                }

                FileOutputStream hashStream = new FileOutputStream(file.getPath().toString() + ".sha1");
                hashStream.write(hashes);
                return true;
            } catch (NoSuchAlgorithmException | IOException e) {
                e.printStackTrace();
                return false;
            }
        }
    }
}
