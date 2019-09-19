package com.cs555.a1.chunkserver;

import com.cs555.a1.Chunk;
import com.cs555.a1.Helper;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class ChunkServer {
    private static class FailureResult {
        ArrayList<SliceFailureRange> sliceFailureRanges = new ArrayList<>();
        byte[] contents;

        FailureResult(byte[] contents) {
            this.contents = contents;
        }

        void add(int offset, int length){
            this.sliceFailureRanges.add(new SliceFailureRange(offset, length));
        }

        private static class SliceFailureRange {
            int offset;
            int length;

            SliceFailureRange(int sliceStart, int sliceStop) {
                this.offset = sliceStart;
                this.length = sliceStop;
            }
        }
    }

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
            boolean justBooted = true;
            while (true) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    System.out.println("Heartbeat thread interrupted, stopping");
                    return;
                }
                if (justBooted || Duration.between(start, Instant.now()).toSeconds() > Helper.MinorHeartbeatSeconds) {
                    justBooted = false;
                    start = Instant.now();
                    if (numMinor == Helper.MajorHeartbeatSeconds / Helper.MinorHeartbeatSeconds) {
                        isMajor = true;
                        numMinor = 0;
                    }
                    System.out.println("Sending heartbeat to controller, ismajor = " + isMajor);
                    try (
                            Socket s = new Socket(controllerMachine, controllerPort);
                            DataOutputStream out = new DataOutputStream(s.getOutputStream())
                    ) {
                        out.writeUTF("heartbeat");
                        out.writeBoolean(isMajor);
                        out.writeInt(Helper.space);
                        out.writeInt(chunks.size());
                        int numChunks = 0;
                        if (!isMajor) {
                            for (String key : chunks.keySet()) {
                                if (chunks.get(key).isNew) {
                                    numChunks++;
                                }
                            }
                        } else {
                            numChunks = chunks.size();
                        }
                        out.writeInt(numChunks);
                        for (String key : chunks.keySet()) {
                            Chunk chunk = chunks.get(key);
                            if (chunk.isNew || isMajor) {
                                chunk.isNew = false;
                                out.writeInt(chunk.version);
                                out.writeUTF(chunk.fileName);
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
                byte[] fileContents = new byte[0];
                String verb = in.readUTF();
                switch (verb) {
                    case "write" :
                        boolean isForwarded = true;
                        fileName = in.readUTF();
                        fileSize = in.readInt();  // likely 64k, but check anyway (could be last chunk)
                        int numForwards = in.readInt();
                        ArrayList<String> forwards = new ArrayList<>();
                        for (int i = 0; i < numForwards; i++)
                            forwards.add(in.readUTF());
                        fileContents = new byte[fileSize];
                        in.readFully(fileContents);
                        boolean isWritten = writeChunk(fileName, fileContents, 0);
                        if (!forwards.isEmpty() && isWritten)
                            isForwarded = Helper.writeToChunkServerWithForward(
                                    fileContents, fileName, forwards, chunkPort);
                        out.writeBoolean(isForwarded && isWritten);
                        break;
                    case "read" :
                        fileName = in.readUTF();
                        int offset = in.readInt();
                        int length = in.readInt();
                        if (chunks.containsKey(fileName)) {
                            FailureResult result = readChunk(fileName, offset, length);
                            if (!result.sliceFailureRanges.isEmpty()) {  //failure detected
                                fileContents = getRecoveredChunk(fileName, fileContents, result);
                            } else {
                                fileContents = result.contents;
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
                        System.out.println("Invalid command received: " + verb);
                        break;
                }
                this.in.close();
                this.out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private byte[] getRecoveredChunk(String fileName, byte[] fileContents, FailureResult result) {
            try {
                for (FailureResult.SliceFailureRange range : result.sliceFailureRanges) {
                    String readServer = Helper.readFromController(
                            controllerMachine, controllerPort, fileName, true, true);
                    fileContents = Helper.readFromChunkServer(
                            fileName, readServer, chunkPort, range.offset, range.length);
                    writeChunk(fileName, fileContents, range.offset);
                }
            } catch (IOException e) {
                System.out.println("Recovery failed");
                e.printStackTrace();
            }
            return fileContents;
        }

        private FailureResult readChunk(String fileName, int offset, int length) {
            try {
                String fullPath = Paths.get(Helper.chunkHome, fileName).toString();
                FileInputStream fileInputStream = new FileInputStream(fullPath);
                byte[] contents;
                long skipped = fileInputStream.skip(offset);
                if (skipped != offset)
                    throw new IOException("Read failure");
                if (length == -1) {
                    contents = fileInputStream.readAllBytes();
                } else
                    contents = fileInputStream.readNBytes(length);
                return validateChunk(fileName, contents, offset);
            } catch (IOException | NoSuchAlgorithmException e) {
                e.printStackTrace();
                FailureResult result = new FailureResult(null);
                result.add(offset, length);
                return result;
            }
        }

        private FailureResult validateChunk(
                String fileName, byte[] contents, int offset) throws IOException, NoSuchAlgorithmException {
            FailureResult result = new FailureResult(contents);
            File file = new File(Paths.get(Helper.chunkHome, fileName).toString());
            FileInputStream hashInputStream = new FileInputStream(file.getPath() + ".sha1");
            byte[] hashes = hashInputStream.readAllBytes();

            int numSlices = contents.length / Helper.BpSlice;
            // if we have extra data beyond the last full 8KB slice, consider it a new slice
            if (contents.length % Helper.BpSlice > 0) {
                numSlices++;
            }

            int hashOffset = (offset / Helper.BpSlice) * Helper.BpHash;
            for (int i = 0; i < numSlices; i++) {
                int hashStart = hashOffset + (i*Helper.BpHash);
                int hashEnd = hashStart+Helper.BpHash;
                int sliceStart = i*Helper.BpSlice;
                int sliceEnd = Integer.min(sliceStart+Helper.BpHash, contents.length);
                if (hashEnd < hashes.length) { // no more hashes, but more slices 
                    result.add(sliceStart, -1);
                    break;
                }

                byte[] oldHash = Arrays.copyOfRange(hashes, hashStart, hashEnd);
                byte[] slice = Arrays.copyOfRange(contents, sliceStart, sliceEnd);
                byte[] newHash = getSHA1(slice);
                if (!Arrays.equals(oldHash, newHash)) {
                    result.add(sliceStart, sliceEnd);
                }
            }
            return result;
        }

        private boolean writeChunk(String fileName, byte[] contents, int slicesOffset) {
            if (slicesOffset % Helper.BpSlice != 0)  //we don't write partial slices
                return false;

            int numSlicesOffset = slicesOffset / Helper.BpSlice;
            int numSlices = contents.length / Helper.BpSlice;
            //we write whole hashes even when chunks are smaller than 64KB, above integer divide would round down
            if (contents.length % Helper.BpSlice != 0)
                numSlices++;

            int hashesOffset = numSlicesOffset * Helper.BpHash;

            try {
                Path chunkHome = Paths.get(Helper.chunkHome);
                if (Files.notExists(chunkHome))
                    Files.createDirectories(chunkHome);

                File file = new File(Paths.get(Helper.chunkHome, fileName).toString());
                if (file.exists() && chunks.containsKey(fileName)) {
                    Chunk chunk = chunks.get(fileName);
                    chunk.version++;
                    chunk.isNew = true;
                } else {
                    Chunk chunk = new Chunk();
                    chunk.fileName = fileName;
                    chunk.version = 1;
                    String[] splits = fileName.split("_");
                    chunk.sequence = Integer.parseInt(splits[splits.length - 1].replace("chunk", ""));
                    chunks.put(fileName, chunk);
                }

                byte[] hashes = new byte[numSlices * Helper.BpHash];
                for (int i = numSlicesOffset; i < numSlicesOffset + numSlices; i++) {
                    int sliceStart = i*Helper.BpSlice;
                    int sliceEnd = Integer.min(sliceStart + Helper.BpSlice, contents.length);
                    byte[] hash = getSHA1(Arrays.copyOfRange(contents, sliceStart, sliceEnd));
                    System.arraycopy(hash, 0, hashes, i*Helper.BpHash, hash.length);
                }

                FileOutputStream hashStream = new FileOutputStream(file.getPath() + ".sha1");
                hashStream.write(hashes, hashesOffset, hashes.length);

                FileOutputStream chunkStream = new FileOutputStream(file.getPath());
                chunkStream.write(contents, slicesOffset, contents.length);
                return true;
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
    }
}
