package com.cs555.a1.client;

import javax.xml.crypto.Data;
import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
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
    private int readLimit = 1000;  // if each chunk is 64KB (64 * 2^10) then this is about 66mB

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
            if (inputs.length == 2) {
                String fileName = inputs[1];
                switch (inputs[0]) {
                    case "quit":
                    case "exit":
                    case "bye":
                        exit = true;
                        break;
                    case "read":
                        if (new File(fileName).exists()) {
                            byte[][] chunks = chunkify(fileName);
                            boolean limitReached = true;
                            boolean failed = false;
                            for (int i = 0; i < this.readLimit; i++) {
                                String chunkFilename = String.format("%s_chunk%s", fileName, Integer.toString(i+1));
                                try (
                                        Socket controllerSocket = new Socket(controllerAddress.getHostName(), controllerAddress.getPort());
                                        DataInputStream controllerIn = new DataInputStream(controllerSocket.getInputStream());
                                        DataOutputStream controllerOut = new DataOutputStream(controllerSocket.getOutputStream());
                                        ) {
                                    controllerOut.writeUTF("read");
                                    controllerOut.writeUTF(chunkFilename);
                                    if (controllerIn.readBoolean()) {
                                        String readServer = controllerIn.readUTF();
                                        controllerIn.close();
                                        controllerOut.close();
                                        controllerSocket.close();
                                        //now proceed to read from chunk server
                                        InetSocketAddress chunkAddress = chunkAddresses.get(readServer);
                                        try (
                                                Socket chunkSocket = new Socket(chunkAddress.getHostName(), chunkAddress.getPort());
                                                DataInputStream chunkIn = new DataInputStream(chunkSocket.getInputStream());
                                                DataOutputStream chunkOut = new DataOutputStream(chunkSocket.getOutputStream());
                                                ) {

                                        } catch (IOException e) {
                                            System.out.println("Couldn't open socket connection to " + chunkAddress.toString());
                                            e.printStackTrace();
                                        }

                                    } else {
                                        //read failed, either we finished reading or the file wasn't found
                                        if (i == 0) {
                                            System.out.println("Controller: File not found.");
                                            failed = true;
                                        }
                                    }

                                } catch (IOException e) {
                                    System.out.println("Couldn't open socket connection to " + controllerAddress.toString());
                                }
                            }
                        } else {
                            System.out.println("Please specify a valid filename");
                        }
                        break;
                    case "write":

                        break;
                    default:
                        System.out.println("Invalid command");
                    case "help":
                        printHelp();
                        break;
                }
            } else {
                System.out.println("Invalid command (should be \"verb object\", for example \"read foo.bar\"");
            }
        }
    }

    private byte[][] chunkify(String fileName) {
        return null; //TODO
    }

    private byte[] dechunkify(byte[][] chunks) {
        return null; //TODO
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
