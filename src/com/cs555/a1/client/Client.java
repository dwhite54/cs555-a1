package com.cs555.a1.client;

import com.cs555.a1.Helper;

import java.io.*;
import java.net.Socket;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.Future;

public class Client {
    private final int chunkPort;
    private final int controllerPort;
    private final String controllerMachine;
    //private AsynchronousSocketChannel controllerClient;
    //private HashMap<String, AsynchronousSocketChannel> chunkClients = new HashMap<String, AsynchronousSocketChannel>();
    private HashMap<String, Future<Void>> chunkFutures = new HashMap<>();

    public Client(int controllerPort, String controllerMachine, int chunkPort) {
        this.controllerMachine = controllerMachine;
        this.controllerPort = controllerPort;
        this.chunkPort = chunkPort;
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
            switch (inputs[0]) {
                case "quit":
                case "exit":
                case "bye":
                    exit = true;
                    break;
                case "read":
                    if (inputs.length == 2) {
                        processRead(inputs[1]);
                        break;
                    }
                case "write":
                    if (inputs.length == 2) {
                        processWrite(inputs[1]);
                        break;
                    }
                default:
                    System.out.println("Invalid verb");
                case "help":
                    printHelp();
                    break;
            }
        }
    }

    private void processWrite(String fileName) {
        ArrayList<byte[]> chunks = chunkify(fileName);
        if (chunks == null)
            return;
        for (int i = 0; i < chunks.size(); i++) {
            String chunkFilename = String.format("%s_chunk%s", fileName, Integer.toString(i+1));
            try (
                    Socket controllerSocket = new Socket(controllerMachine, controllerPort);
                    DataInputStream controllerIn = new DataInputStream(controllerSocket.getInputStream());
                    DataOutputStream controllerOut = new DataOutputStream(controllerSocket.getOutputStream())
            ) {
                controllerOut.writeUTF("write");
                controllerOut.writeUTF(chunkFilename);
                int numServers = controllerIn.readInt();
                ArrayList<String> chunkServers = new ArrayList<>();
                for (int j = 0; j < numServers; j++)
                    chunkServers.add(controllerIn.readUTF());
                controllerIn.close();
                controllerOut.close();
                controllerSocket.close();
                //now proceed to write to chunk server (with forwarding info)
                if (!Helper.writeToChunkServerWithForward(chunks.get(i), chunkFilename, chunkServers, chunkPort))
                    break;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void processRead(String fileName) {
        if (new File(fileName).exists()) {
            ArrayList<byte[]> chunks = new ArrayList<>();
            boolean limitReached = true;
            boolean failed = false;
            for (int i = 0; i < Helper.readLimit; i++) {
                byte[] readChunk;
                String chunkFilename = String.format("%s_chunk%s", fileName, Integer.toString(i+1));
                try (
                        Socket controllerSocket = new Socket(controllerMachine, controllerPort);
                        DataInputStream controllerIn = new DataInputStream(controllerSocket.getInputStream());
                        DataOutputStream controllerOut = new DataOutputStream(controllerSocket.getOutputStream())
                ) {
                    controllerOut.writeUTF("read");
                    controllerOut.writeUTF(chunkFilename);
                    if (controllerIn.readBoolean()) {
                        String readServer = controllerIn.readUTF();
                        controllerIn.close();
                        controllerOut.close();
                        controllerSocket.close();
                        //now proceed to read from chunk server
                        try (
                                Socket chunkSocket = new Socket(readServer, chunkPort);
                                DataInputStream chunkIn = new DataInputStream(chunkSocket.getInputStream());
                                DataOutputStream chunkOut = new DataOutputStream(chunkSocket.getOutputStream())
                        ) {
                            chunkOut.writeUTF(chunkFilename);
                            int readChunkSize = chunkIn.readInt();
                            readChunk = new byte[readChunkSize];
                            int bytesRead = chunkIn.read(readChunk);
                            if (readChunkSize != bytesRead) {
                                System.out.println("Failed reading file from chunk server: " + chunkFilename);
                                failed = true;
                                break;
                            } else {
                                chunks.add(readChunk);
                                limitReached = false;
                            }
                        } catch (IOException e) {
                            System.out.println("Couldn't open socket connection to " + readServer + ":" + chunkPort);
                            e.printStackTrace();
                            failed = true;
                            break;
                        }

                    } else {
                        if (i == 0) {  // file not found
                            System.out.println("Controller: File not found.");
                            failed = true;
                            break;
                        } else {  // read finished
                            break;
                        }
                    }

                } catch (IOException e) {
                    System.out.println("Couldn't open socket connection to " + controllerMachine + ":" + controllerPort);
                }
            }
            if (limitReached) {
                System.out.println("File too large (maximum number of chunks exceeded)");
                failed = true;
            }
            if (!failed) {
                dechunkify(fileName, chunks); // writes file to disk
            }
        } else {
            System.out.println("Please specify a valid filename");
        }
    }

    private ArrayList<byte[]> chunkify(String fileName) {
        ArrayList<byte[]> chunks = new ArrayList<>();
        try {
            FileInputStream fileInputStream = new FileInputStream(fileName);
            byte[] contents = fileInputStream.readAllBytes();
            int numChunks = contents.length / Helper.BpChunk;
            if (contents.length % Helper.BpChunk != 0) numChunks++;
            for (int i = 0; i < numChunks; i++) {
                int startIdx = i*Helper.BpChunk;
                int endIdx = Integer.min((i*Helper.BpChunk)+Helper.BpChunk, contents.length - 1);
                chunks.add(Arrays.copyOfRange(contents, startIdx, endIdx));
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return chunks;
    }

    private void dechunkify(String fileName, ArrayList<byte[]> chunks) {
        try ( FileOutputStream fileOutputStream = new FileOutputStream(fileName)) {
            for (byte[] chunk : chunks)
                fileOutputStream.write(chunk);
        } catch (IOException e) {
            System.out.println("Client: Error writing file.");
            e.printStackTrace();
        }
    }

    private void printHelp() {
        System.out.println("Available commands:");
        System.out.println("\t[quit,exit,bye]: Exit the program.");
        System.out.println("\twrite foo.bar: ");
        System.out.println("\tread foo.bar");
        System.out.println("\thelp: print this list.");
    }
}
