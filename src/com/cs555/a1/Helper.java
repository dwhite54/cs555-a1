package com.cs555.a1;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;

public class Helper {
    public static int BpSlice = 1024*8;
    public static int BpHash = 20;
    public static int slicesPerChunk = 8;
    public static int BpChunk = BpSlice * slicesPerChunk;
    public static String chunkHome = "/tmp/dwhite54/chunks";
    public static int space = 100;
    public static int readLimit = 1000;  // if each chunk is 64KB (64 * 2^10) then this is about 66mB
    public static int replicationFactor = 1;
    public static int MajorHeartbeatSeconds = 60;//300;
    public static int MinorHeartbeatSeconds = 6;//30;

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
            chunkOut.write(chunk);
            chunkOut.writeInt(chunkServers.size());
            for (String server : chunkServers)
                chunkOut.writeUTF(server);
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
