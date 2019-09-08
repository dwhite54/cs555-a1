package com.cs555.a1.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.sql.Array;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Client {
    private AsynchronousSocketChannel controllerClient;
    private HashMap<String, AsynchronousSocketChannel> chunkClients = new HashMap<String, AsynchronousSocketChannel>();
    private HashMap<String, Future<Void>> chunkFutures = new HashMap<String, Future<Void>>();
    private InetSocketAddress controllerAddress;
    private HashMap<String, InetSocketAddress> chunkAddresses = new HashMap<String, InetSocketAddress>();

    public Client(int controllerPort, String controllerMachine, int chunkPort, String[] chunkMachines) {
        try {
            controllerAddress = new InetSocketAddress(controllerMachine, controllerPort);
            controllerClient = AsynchronousSocketChannel.open();
            for (String chunkMachine : chunkMachines) {
                chunkAddresses.put(chunkMachine, new InetSocketAddress(chunkMachine, chunkPort));
                chunkClients.put(chunkMachine, AsynchronousSocketChannel.open());
            }
        } catch (IOException e) {
            System.out.println("Can't initiate socket connection objects. Aborting.");
            e.printStackTrace();
        }
    }

    public void run() {
        Scanner scanner = new Scanner(System.in);
        String input = "";
        boolean exit = false;
        System.out.println("Type a command and hit return. To see available commands, type 'help'");
        while (!exit) {
            System.out.print(">");
            input = scanner.nextLine();
            String[] inputs = input.split(" ");
            if (inputs.length > 0) {
                switch (inputs[0]) {
                    case "quit":
                    case "exit":
                    case "bye":
                        exit = true;
                        break;
                    case "help":
                        printHelp();
                        break;
                    case "info":
                    case "read":
                    case "write":
                        System.out.println("Not implemented");
                        break;
                    case "ping":
                        System.out.println("Sending ping...");
                        String message = "PING";
                        String out = startWriteRecvAndStop(message, controllerClient, controllerAddress);
                        System.out.println("Received " + out);
                }
            } else {
                System.out.println("Please enter a command");
            }
        }
    }

    private String startWriteRecvAndStop(String message, AsynchronousSocketChannel client, InetSocketAddress address) {
        String out;
        start(client, address);
        out = sendMessage(message, client);
        stop(client);
        return out;
    }

    private void start(AsynchronousSocketChannel client, InetSocketAddress address) {
        try {
            Future<Void> future = client.connect(address);
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private String sendMessage(String message, AsynchronousSocketChannel client) {
        byte[] byteMsg = message.getBytes();
        ByteBuffer buffer = ByteBuffer.wrap(byteMsg);
        Future<Integer> writeResult = client.write(buffer);

        try {
            writeResult.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        buffer.flip();
        Future<Integer> readResult = client.read(buffer);
        try {
            readResult.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        String echo = new String(buffer.array()).trim();
        buffer.clear();
        return echo;
    }

    private void stop(AsynchronousSocketChannel client) {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void printHelp() {
        System.out.println("Available commands:");
        System.out.println("\t[quit,exit,bye]: Exit the program.");
        System.out.println("\tinfo: .");
        System.out.println("\tinfo: .");
        System.out.println("\twrite: .");
        System.out.println("\tread: .");
        System.out.println("\thelp: print this list.");
    }
}
