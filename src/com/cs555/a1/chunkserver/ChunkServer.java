package com.cs555.a1.chunkserver;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class ChunkServer {
    public ChunkServer(int controllerPort, String controllerMachine, int chunkPort, String[] chunkMachines) {
        String hostname = "unknown";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            System.out.println("Unknown host.");
            System.out.println(e.getMessage());
        }
        System.out.print(String.format("Hello from ChunkServer on %s!%n", hostname));
    }

    public void run() {
    }
}
