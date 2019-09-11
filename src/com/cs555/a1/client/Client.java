package com.cs555.a1.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.sql.Array;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Client {
    //private AsynchronousSocketChannel controllerClient;
    //private HashMap<String, AsynchronousSocketChannel> chunkClients = new HashMap<String, AsynchronousSocketChannel>();
    private HashMap<String, Future<Void>> chunkFutures = new HashMap<>();
    private InetSocketAddress controllerAddress;
    private HashMap<String, InetSocketAddress> chunkAddresses = new HashMap<>();

    public Client(int controllerPort, String controllerMachine, int chunkPort, String[] chunkMachines) {
        controllerAddress = new InetSocketAddress(controllerMachine, controllerPort);
        //controllerClient = AsynchronousSocketChannel.open();
        for (String chunkMachine : chunkMachines) {
            chunkAddresses.put(chunkMachine, new InetSocketAddress(chunkMachine, chunkPort));
            //chunkClients.put(chunkMachine, AsynchronousSocketChannel.open());
        }
    }

    public void run() {
        Scanner scanner = new Scanner(System.in);
        String input;
        boolean exit = false;
        System.out.println("Type a command and hit return. To see available commands, type 'help'");
        while (!exit) {
            System.out.print(">");
            input = scanner.nextLine();
            String[] inputs = input.split(" ");
            String out = "";
            if (inputs.length > 0) {
                switch (inputs[0]) {
                    case "quit":
                    case "exit":
                    case "bye":
                        exit = true;
                        break;
                    case "read":
                        out = fn(controllerAddress, "read");
                        //pass this along to chunk server from out
                        break;
                    case "write":
                        out = fn(controllerAddress, "write");
                        //pass this along to chunk server from out
                        break;
                    default:
                        System.out.println("Invalid command");
                    case "help":
                        printHelp();
                        break;
                }
            } else {
                System.out.println("Please enter a command");
            }
            if (!out.equals(""))
                System.out.println("Received: " + out);
        }
    }

    private String fn(InetSocketAddress address, String message) {
        String received = "ERROR";
        try
        {
            Socket s = new Socket(address.getHostName(), address.getPort());

            DataInputStream dis = new DataInputStream(s.getInputStream());
            DataOutputStream dos = new DataOutputStream(s.getOutputStream());

            dos.writeUTF(message);
            received = dis.readUTF();

            // closing resources
            dis.close();
            dos.close();
        }catch(Exception e){
            e.printStackTrace();
        }

        return received;
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
