package com.cs555.a1;

import erasure.ReedSolomon;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

public class Helper {
    public static int BpSlice = 1024*8;
    public static int BpHash = 20;
    public static int slicesPerChunk = 8;
    public static int BpChunk = BpSlice * slicesPerChunk;
    public static String chunkHome = "/tmp/dwhite54/chunks";
    public static int space = 10000;
    public static int readLimit = 1000;  // if each chunk is 64KB (64 * 2^10) then this is about 66mB
    public static int MajorHeartbeatSeconds = 300;
    public static int MinorHeartbeatSeconds = 30;
    public static boolean debug = true;
    public static boolean useReplication = false;

    public static final int DATA_SHARDS = 6;
    public static final int PARITY_SHARDS = 3;
    public static final int TOTAL_SHARDS = 9;
    public static final int BYTES_IN_INT = 4;

    private static int _replicationFactor = 3;
    public static int replicationFactor = useReplication ? _replicationFactor : 1;

    public static byte[][] erasureEncode(byte[] input) {
        final int storedSize = input.length + BYTES_IN_INT;
        final int shardSize = (storedSize + DATA_SHARDS - 1) / DATA_SHARDS;
        int bufferSize = shardSize * DATA_SHARDS;
        byte[] allBytes = new byte[bufferSize];
        ByteBuffer.wrap(allBytes).putInt(input.length);
        System.arraycopy(input, 0, allBytes, BYTES_IN_INT, input.length);
        byte[][] shards = new byte[Helper.TOTAL_SHARDS][shardSize];
        for (int i = 0; i < Helper.DATA_SHARDS; i++) {
            System.arraycopy(allBytes, i * shardSize, shards[i], 0, shardSize);
        }
        ReedSolomon reedSolomon = new ReedSolomon(Helper.DATA_SHARDS, Helper.PARITY_SHARDS);
        reedSolomon.encodeParity(shards, 0, shardSize);
        return shards;
    }

    public static byte[] erasureDecode(byte[][] shards, boolean[] shardPresent, int shardSize) {
        int shardCount = 0;
        for (int i = 0; i < shardPresent.length; i++) {
            if (shardPresent[i]) {
                shardCount++;
            } else {
                shards[i] = new byte[shardSize];
            }
        }
        if (shardCount < DATA_SHARDS) {
            System.out.println("Not enough shards present");
            return null;
        }
        ReedSolomon reedSolomon = new ReedSolomon(Helper.DATA_SHARDS, Helper.PARITY_SHARDS);
        reedSolomon.decodeMissing(shards, shardPresent, 0, shardSize);
        byte [] allBytes = new byte [shardSize * DATA_SHARDS];
        for (int i = 0; i < DATA_SHARDS; i++) {
            System.arraycopy(shards[i], 0, allBytes, shardSize * i, shardSize);
        }
        // Extract the file length
        int fileSize = ByteBuffer.wrap(allBytes).getInt();
        byte[] output = new byte[fileSize];
        System.arraycopy(allBytes, BYTES_IN_INT, output, 0, output.length);
        return output;
    }

    public static byte[] getSHA1(byte[] chunk) throws NoSuchAlgorithmException {
        MessageDigest crypt = MessageDigest.getInstance("SHA-1");
        crypt.reset();
        crypt.update(chunk);
        return crypt.digest();
    }

    //https://stackoverflow.com/questions/9655181/how-to-convert-a-byte-array-to-a-hex-string-in-java
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    public static boolean writeToChunkServerWithForward(
            byte[] chunk, String chunkFilename, ArrayList<String> chunkServers, int chunkPort) {
        String chunkServer = chunkServers.get(0);
        chunkServers.remove(0);
        try (
                Socket chunkSocket = new Socket(chunkServer, chunkPort);
                DataInputStream chunkIn = new DataInputStream(chunkSocket.getInputStream());
                DataOutputStream chunkOut = new DataOutputStream(chunkSocket.getOutputStream())
        ) {
            chunkOut.writeUTF("write");
            System.out.println("writing " + chunkFilename + " to " + chunkServer + " with forward " + chunkServers.toString());
            chunkOut.writeUTF(chunkFilename);
            chunkOut.writeInt(chunk.length);
            chunkOut.writeInt(chunkServers.size());
            for (String server : chunkServers)
                chunkOut.writeUTF(server);
            if (chunk.length > 0)
                chunkOut.write(chunk);
            if (!chunkIn.readBoolean()) {
                System.out.println("Failed writing file to chunk server: " + chunkFilename);
                return false;
            }
        } catch (IOException e) {
            System.out.println("Couldn't open socket connection to " + chunkServer + ":" + chunkPort);
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static byte[] readFromChunkServer(
            String chunkFilename, String chunkServer, int chunkPort, int offset, int length) throws IOException {
        try (
                Socket chunkSocket = new Socket(chunkServer, chunkPort);
                DataInputStream chunkIn = new DataInputStream(chunkSocket.getInputStream());
                DataOutputStream chunkOut = new DataOutputStream(chunkSocket.getOutputStream())
        ) {
            chunkOut.writeUTF("read");
            chunkOut.writeUTF(chunkFilename);
            chunkOut.writeInt(offset);
            chunkOut.writeInt(length);
            int fileSize = chunkIn.readInt();
            if (fileSize == 0) {
                return null;
            }
            byte[] chunk = new byte[fileSize];
            chunkIn.readFully(chunk);
            return chunk;
        }
    }

    public static String readFromController(
            String controllerMachine, int controllerPort, String fileName, boolean isFailure, boolean isChunkServer
    ) throws IOException {
        try (
                Socket controllerSocket = new Socket(controllerMachine, controllerPort);
                DataInputStream controllerIn = new DataInputStream(controllerSocket.getInputStream());
                DataOutputStream controllerOut = new DataOutputStream(controllerSocket.getOutputStream())
        ) {
            controllerOut.writeUTF("read");
            controllerOut.writeUTF(fileName);
            controllerOut.writeBoolean(isFailure);
            controllerOut.writeBoolean(isChunkServer);
            if (controllerIn.readBoolean()) {
                return controllerIn.readUTF();
            } else {
                return null;
            }
        }
    }

    public static void processTaddle(
            String controllerMachine, int controllerPort, String fileName, ArrayList<String> machines) throws IOException {
        try (
                Socket controllerSocket = new Socket(controllerMachine, controllerPort);
                DataInputStream controllerIn = new DataInputStream(controllerSocket.getInputStream());
                DataOutputStream controllerOut = new DataOutputStream(controllerSocket.getOutputStream())
        ) {
            controllerOut.writeUTF("taddle");
            controllerOut.writeUTF(fileName);
            controllerOut.writeInt(machines.size());
            for (String machine : machines)
                controllerOut.writeUTF(machine);
        }
    }
}
