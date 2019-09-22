package com.cs555.a1.client;

import com.cs555.a1.Helper;

import java.io.*;
import java.net.Socket;
import java.util.*;

public class Client {
    private final int chunkPort;
    private final int controllerPort;
    private final String controllerMachine;

    public Client(int controllerPort, String controllerMachine, int chunkPort) {
        this.controllerMachine = controllerMachine;
        this.controllerPort = controllerPort;
        this.chunkPort = chunkPort;
    }

    public void run() {
        Scanner scanner = new Scanner(System.in);
        String input;
        boolean exit = false;
        boolean result = false;
        System.out.println("Type a command and hit return. To see available commands, type 'help'");
        while (!exit) {
            System.out.print(">");
            input = scanner.nextLine();
            String[] inputs = input.split(" ");
            switch (inputs[0]) {
                case "quit":
                case "exit":
                case "bye":
                    exit = true;
                    break;
                case "read":
                    if (inputs.length == 2) {
                        result = processRead(inputs[1]);
                        break;
                    }
                case "write":
                    if (inputs.length == 2) {
                        result = processWrite(inputs[1]);
                        break;
                    }
                default:
                    System.out.println("Invalid verb");
                case "help":
                    printHelp();
                    break;
            }

            if (result) {
                System.out.println("Success.");
            } else {
                System.out.println("Failure.");
            }
        }
    }

    private boolean processWrite(String fileName) {
        boolean result = true;
        ArrayList<byte[]> chunks = chunkify(fileName);
        if (chunks == null) {
            System.out.println("Error reading file from disk");
            result = false;
        } else {
            for (int i = 0; i < chunks.size(); i++) {
                String chunkFilename = String.format("%s_chunk%s", fileName, Integer.toString(i + 1));
                try (
                        Socket controllerSocket = new Socket(controllerMachine, controllerPort);
                        DataInputStream controllerIn = new DataInputStream(controllerSocket.getInputStream());
                        DataOutputStream controllerOut = new DataOutputStream(controllerSocket.getOutputStream())
                ) {
                    controllerOut.writeUTF("write");
                    controllerOut.writeUTF(chunkFilename);
                    if (!controllerIn.readBoolean()) {
                        System.out.println("Controller not accepting writes.");
                        result = false;
                        break;
                    }
                    int numServers = controllerIn.readInt();
                    ArrayList<String> chunkServers = new ArrayList<>();
                    for (int j = 0; j < numServers; j++)
                        chunkServers.add(controllerIn.readUTF());
                    controllerIn.close();
                    controllerOut.close();
                    controllerSocket.close();
                    //now proceed to write to chunk server (with forwarding info)
                    if (!Helper.writeToChunkServerWithForward(chunks.get(i), chunkFilename, chunkServers, chunkPort)) {
                        result = false;
                        break;
                    }
                } catch (IOException e) {
                    System.out.println("Error opening socket connection.");
                    e.printStackTrace();
                    result = false;
                }
            }
        }
        return result;
    }

    private boolean processRead(String fileName) {
        boolean isSuccess = false;
        try {
            ArrayList<byte[]> chunks = new ArrayList<>();
            for (int i = 0; i < Helper.readLimit; i++) {
                byte[] readChunk;
                String chunkFilename = String.format("%s_chunk%s", fileName, Integer.toString(i + 1));
                String readServer = Helper.readFromController(
                        controllerMachine, controllerPort, chunkFilename, false, false);
                if (readServer == null)
                    break;
                if (Helper.debug) System.out.println("reading chunk from " + readServer);
                readChunk = Helper.readFromChunkServer(chunkFilename, readServer, chunkPort, 0, -1);
                chunks.add(readChunk);
                if (Helper.debug) System.out.println("Successfully read " + chunkFilename);
            }
            if (chunks.size() == Helper.readLimit) {
                System.out.println("File too large (maximum number of chunks exceeded)");
            } else if (chunks.size() == 0) {
                System.out.println("File not found");
            } else {
                dechunkify(fileName, chunks); // writes file to disk
                isSuccess = true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return isSuccess;
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
                int endIdx = Integer.min((i*Helper.BpChunk)+Helper.BpChunk, contents.length);
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
            if (Helper.debug) System.out.println("Client: Error writing file.");
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
