package com.cs555.a1.chunkserver;

import com.cs555.a1.Chunk;
import com.cs555.a1.Helper;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

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
    private volatile boolean needHeartbeat;
    //need to store the chunks that are at this server (filename with underscore and integer appended),
    //for each chunk we need version, and 8 SHA-1 hashes (1 per 8KB),
    private final ConcurrentHashMap<String, Chunk> chunks;

    public ChunkServer(int controllerPort, String controllerMachine, int chunkPort) throws IOException {
        this.controllerPort = controllerPort;
        this.controllerMachine = controllerMachine;
        this.chunkPort = chunkPort;
        this.chunks = new ConcurrentHashMap<>();
        this.needHeartbeat = true;
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
        Thread mT = new ChunkHeartbeatHandler();
        mT.start();
        while (!shutdown)
        {
            Socket s = null;
            Thread.sleep(100);
            try
            {
                s = ss.accept();
                DataInputStream dis = new DataInputStream(s.getInputStream());
                DataOutputStream dos = new DataOutputStream(s.getOutputStream());
                Thread t = new ChunkRequestHandler(dis, dos);
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

    private class ChunkHeartbeatHandler extends Thread {
        @Override
        public void run() {
            Instant start = Instant.now();
            int numMinor = 0;
            boolean isMajor;
            boolean firstRun = true;
            while (true) {
                if (needHeartbeat || Duration.between(start, Instant.now()).toSeconds() > Helper.MinorHeartbeatSeconds){
                    start = Instant.now();
                    if (firstRun || (numMinor >= Helper.MajorHeartbeatSeconds / Helper.MinorHeartbeatSeconds && !needHeartbeat)) {
                        firstRun = false;
                        isMajor = true;
                        numMinor = 0;
                    } else {
                        isMajor = false;
                        numMinor++;
                    }
                    System.out.println("Sending heartbeat to controller, ismajor = " + isMajor);
                    try (
                            Socket s = new Socket(controllerMachine, controllerPort);
                            DataOutputStream out = new DataOutputStream(s.getOutputStream())
                    ) {
                        out.writeUTF("heartbeat");
                        out.writeBoolean(isMajor);
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
                        System.out.println("Error while sending heartbeat.");
                        e.printStackTrace();
                        dumpStack();
                    }
                    needHeartbeat = false;
                }
            }
        }
    }

    // ClientHandler class
    class ChunkRequestHandler extends Thread
    {
        private final DataInputStream in;
        private final DataOutputStream out;

        // Constructor
        ChunkRequestHandler(DataInputStream in, DataOutputStream out)
        {
            this.in = in;
            this.out = out;
        }

        @Override
        public void run()
        {
            try {
                String verb = in.readUTF();
                switch (verb) {
                    case "write":
                        handleWrite();
                        break;
                    case "read":
                        handleRead();
                        break;
                    case "heartbeat":  // tell the controller we are still here
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

        private void handleRead() throws IOException {
            String fileName;
            byte[] fileContents;
            fileName = in.readUTF();
            int offset = in.readInt();
            int length = in.readInt();
            if (chunks.containsKey(fileName)) {
                FailureResult result = readChunk(fileName, offset, length);
                if (!result.sliceFailureRanges.isEmpty()) {  //failure detected
                    System.out.println("failure detected for " + fileName);
                    FailureResult recoverResult = getRecoveredChunk(fileName, result, offset, length);
                    if (recoverResult != null && recoverResult.sliceFailureRanges.isEmpty())
                        fileContents = recoverResult.contents;
                    else {
                        fileContents = null;
                    }
                } else {
                    fileContents = result.contents;
                }
                //now respond to requester
                if (fileContents == null) { // recovery failed
                    out.writeInt(0);
                    chunks.remove(fileName);
                }
                else {
                    System.out.println("Serving file to client");
                    out.writeInt(fileContents.length);
                    out.write(fileContents);
                }
            } else {
                out.writeInt(0);
            }
        }

        private void handleWrite() throws IOException {
            String fileName;
            int fileSize;
            byte[] fileContents = new byte[0];
            boolean isForwarded = true;
            boolean isWritten = true;
            fileName = in.readUTF();
            fileSize = in.readInt();  // likely 64k, but check anyway (could be last chunk)
            int numForwards = in.readInt();
            ArrayList<String> forwards = new ArrayList<>();
            for (int i = 0; i < numForwards; i++)
                forwards.add(in.readUTF());
            if (fileSize == 0) { // this signals we should have the file
                FailureResult result = readChunk(fileName, 0, -1);
                if (result.sliceFailureRanges.isEmpty())
                    fileContents = result.contents;
                else
                    isWritten = false;
            } else {
                fileContents = new byte[fileSize];
                in.readFully(fileContents);
                isWritten = writeChunk(fileName, fileContents, 0);
            }
            if (!forwards.isEmpty() && isWritten)
                isForwarded = Helper.writeToChunkServerWithForward(
                        fileContents, fileName, forwards, chunkPort);
            out.writeBoolean(isForwarded && isWritten);
        }

        private FailureResult getRecoveredChunk(String fileName, FailureResult result, int originalOffset, int originalLength) {
            try {
                for (FailureResult.SliceFailureRange range : result.sliceFailureRanges) {
                    String readServer = Helper.readFromController(
                            controllerMachine, controllerPort, fileName, true, true);
                    if (readServer == null)
                        throw new IOException("Controller request denied.");
                    if (Helper.debug)
                        System.out.printf("Requesting slice/chunk from %s offset %d length %d%n",
                                readServer, range.offset, range.length);
                    byte[] recovered = Helper.readFromChunkServer(
                            fileName, readServer, chunkPort, range.offset, range.length);
                    writeChunk(fileName, recovered, range.offset);
                }
                return readChunk(fileName, originalOffset, originalLength);
            } catch (IOException e) {
                System.out.println("Recovery failed");
                e.printStackTrace();
                return null;
            }
        }

        private FailureResult readChunk(String fileName, int offset, int length) {
            try {
                String fullPath = Paths.get(Helper.chunkHome, fileName).toString();
                FileInputStream fileInputStream = new FileInputStream(fullPath);
                if (!Helper.useReplication)
                    return new FailureResult(fileInputStream.readAllBytes());
                byte[] contents;
                System.out.printf("Reading file %s offset %d length %d%n", fileName, offset, length);
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
                if (Helper.useReplication)
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
                int sliceEnd = Integer.min(sliceStart+Helper.BpSlice, contents.length);
                if (hashEnd > hashes.length) { // no more hashes, but more slices
                    result.add(sliceStart, -1);
                    break;
                }

                byte[] oldHash = Arrays.copyOfRange(hashes, hashStart, hashEnd);
                byte[] slice = Arrays.copyOfRange(contents, sliceStart, sliceEnd);
                byte[] newHash = Helper.getSHA1(slice);
                if (!Arrays.equals(oldHash, newHash)) {
                    if (sliceEnd == contents.length)
                        result.add(sliceStart, -1);
                    else
                        result.add(sliceStart, sliceEnd-sliceStart);
                }
            }
            return result;
        }

        private boolean writeChunk(String fileName, byte[] contents, int slicesOffset) {
            try {
                Path chunkHome = Paths.get(Helper.chunkHome);
                if (Files.notExists(chunkHome))
                    Files.createDirectories(chunkHome);

                File file = new File(Paths.get(Helper.chunkHome, fileName).toString());
                synchronized (chunks) {
                    if (file.exists() && chunks.containsKey(fileName)) {
                        Chunk chunk = chunks.get(fileName);
                        if (slicesOffset == 0) //an update, not a recovery
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
                }

                if (Helper.useReplication) {
                    if (slicesOffset % Helper.BpSlice != 0)  //we don't write partial slices
                        return false;

                    int numSlicesOffset = slicesOffset / Helper.BpSlice;
                    int numSlices = contents.length / Helper.BpSlice;
                    //we write whole hashes even when chunks are smaller than 64KB, above integer divide would round down
                    if (contents.length % Helper.BpSlice != 0)
                        numSlices++;

                    int hashesOffset = numSlicesOffset * Helper.BpHash;

                    byte[] hashes = new byte[numSlices * Helper.BpHash];
                    for (int i = 0; i < numSlices; i++) {
                        int sliceStart = i * Helper.BpSlice;
                        int sliceEnd = Integer.min(sliceStart + Helper.BpSlice, contents.length);
                        byte[] hash = Helper.getSHA1(Arrays.copyOfRange(contents, sliceStart, sliceEnd));
                        System.arraycopy(hash, 0, hashes, i * Helper.BpHash, hash.length);
                    }

                    RandomAccessFile hashStream = new RandomAccessFile(file.getPath() + ".sha1", "rw");
                    hashStream.seek(hashesOffset);
                    hashStream.write(hashes);
                }
                RandomAccessFile chunkStream = new RandomAccessFile(file.getPath(), "rw");
                chunkStream.seek(slicesOffset);
                chunkStream.write(contents);
                //needHeartbeat = true;
                return true;
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
    }
}
