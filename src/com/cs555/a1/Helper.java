package com.cs555.a1;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
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
    public static int replicationFactor = 3;
    public static int MajorHeartbeatSeconds = 60;//300;
    public static int MinorHeartbeatSeconds = 1;//30;
    public static boolean debug = true;

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
            if (Helper.debug) System.out.println("writing " + chunkFilename + " to " + chunkServer + " with forward " + chunkServers.toString());
            chunkOut.writeUTF(chunkFilename);
            chunkOut.writeInt(chunk.length);
            chunkOut.writeInt(chunkServers.size());
            for (String server : chunkServers)
                chunkOut.writeUTF(server);
            if (chunk.length > 0)
                chunkOut.write(chunk);
            if (!chunkIn.readBoolean()) {
                if (Helper.debug) System.out.println("Failed writing file to chunk server: " + chunkFilename);
                return false;
            }
        } catch (IOException e) {
            if (Helper.debug) System.out.println("Couldn't open socket connection to " + chunkServer + ":" + chunkPort);
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
                throw new IOException("File read error");
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
}
