package com.cs555.a1.controller;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.*;
import java.util.concurrent.Future;

public class Controller {
    public int ControllerPort = 0;
    public String ControllerMachine = "";
    public Controller(int controllerPort, String controllerMachine) {
        ControllerPort = controllerPort;
        ControllerMachine = controllerMachine;
        ChunkPort = chunkPort;
        ChunkMachines = chunkMachines;
        String hostname = "unknown";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            System.out.println("Unknown host.");
            System.out.println(e.getMessage());
        }
        System.out.print(String.format("Hello from Controller on %s!%n", hostname));
    }

    public void run() throws IOException {
        AsynchronousServerSocketChannel server = AsynchronousServerSocketChannel.open();
        server.bind(new InetSocketAddress(ControllerMachine, ControllerPort));
        Future<AsynchronousSocketChannel> acceptFuture = server.accept();
        AsynchronousSocketChannel worker = future.get();
    }
}
