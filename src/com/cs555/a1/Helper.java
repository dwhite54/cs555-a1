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

    public static boolean writeToChunkServerWithForward(byte[] chunk, String chunkFilename, ArrayList<String> chunkServers, int chunkPort) {
        String chunkServer = chunkServers.get(0);
        chunkServers.remove(0);
        try (
                Socket chunkSocket = new Socket(chunkServer, chunkPort);
                DataInputStream chunkIn = new DataInputStream(chunkSocket.getInputStream());
                DataOutputStream chunkOut = new DataOutputStream(chunkSocket.getOutputStream())
        ) {
            chunkOut.writeUTF(chunkFilename);
            chunkOut.writeInt(chunk.length);
            chunkOut.write(chunk);
            chunkOut.write(chunkServers.size());
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
}
