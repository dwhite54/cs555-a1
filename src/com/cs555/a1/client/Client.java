package com.cs555.a1.client;

import com.cs555.a1.Helper;

import java.io.*;
import java.net.Socket;
import java.util.*;

import static com.cs555.a1.Helper.TOTAL_SHARDS;

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
        byte[][][] chunks = chunkify(fileName);
        if (chunks == null) {
            System.out.println("Error reading file from disk");
            return false;
        } else {
            for (int i = 0; i < chunks.length; i++) {
                if (!disperseChunk(fileName + "." + i, chunks[i]))
                    return false;
            }
        }
        return true;
    }

    private boolean disperseChunk(String chunkFilename, byte[] chunk) {  // ugly
        byte[][] newChunk = new byte[1][chunk.length];
        System.arraycopy(chunk, 0, newChunk[0], 0, chunk.length);
        return disperseChunk(chunkFilename, newChunk);
    }

    private boolean disperseChunk(String chunkFilename, byte[][] chunk) {
        byte[][] shards;
        if (Helper.useReplication) {
            shards = chunk;
        } else {
            shards = Helper.erasureEncode(chunk[0]);
        }
        for (int j = 0; j < shards.length; j++) {
            String shardFilename;
            shardFilename = chunkFilename + "." + j;
            try (
                    Socket controllerSocket = new Socket(controllerMachine, controllerPort);
                    DataInputStream controllerIn = new DataInputStream(controllerSocket.getInputStream());
                    DataOutputStream controllerOut = new DataOutputStream(controllerSocket.getOutputStream())
            ) {
                controllerOut.writeUTF("write");
                controllerOut.writeUTF(shardFilename);
                if (!controllerIn.readBoolean()) {
                    System.out.println("Controller not accepting writes.");
                    return false;
                }
                int numServers = controllerIn.readInt();
                ArrayList<String> chunkServers = new ArrayList<>();
                for (int k = 0; k < numServers; k++)
                    chunkServers.add(controllerIn.readUTF());
                controllerIn.close();
                controllerOut.close();
                controllerSocket.close();
                //now proceed to write to chunk server (with forwarding info)
                if (!Helper.writeToChunkServerWithForward(shards[j], shardFilename, chunkServers, chunkPort)) {
                    Helper.processTaddle(controllerMachine, controllerPort, shardFilename, chunkServers);
                    return false;
                }
            } catch (IOException e) {
                System.out.println("Error opening socket connection.");
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    private boolean processRead(String fileName) {
        try {
            ArrayList<byte[]> chunks = new ArrayList<>();
            int shardLimit = Helper.useReplication ? 1 : TOTAL_SHARDS;
            chunkloop: for (int i = 0; i < Helper.readLimit; i++) {
                boolean needsRedispersed = false;
                ArrayList<byte[]> shards = new ArrayList<>();
                String chunkFilename = fileName + "." + i;
                shardloop: for (int j = 0; j < shardLimit; j++) {
                    String shardFilename = chunkFilename + "." + j;
                    String readServer = Helper.readFromController(
                            controllerMachine, controllerPort, shardFilename, false, false);
                    if (readServer == null) {  // controller can't find file
                        if (!Helper.useReplication) { // if erasure, we know it's there, so call it missing
                            shards.add(null);
                            needsRedispersed = true;
                            continue shardloop;
                        } else if (i == 0) {
                            System.out.println("File not found");
                            return false;
                        } else { //we've reached the end of the chunks (and using replication), so exit outer loop
                            break chunkloop;
                        }
                    }

                    System.out.println("Reading " + shardFilename + " from " + readServer);
                    byte[] readChunk = Helper.readFromChunkServer(shardFilename, readServer, chunkPort, 0, -1);
                    if (Helper.useReplication && readChunk == null)
                        return false;
                    else if (readChunk == null)
                        needsRedispersed = true;
                    shards.add(readChunk);
                }
                if (Helper.useReplication)
                    chunks.add(shards.get(0));
                else {
                    boolean foundShard = false;
                    for (byte[] shard : shards) {
                        if (shard != null) {
                            foundShard = true;
                            break;
                        }
                    }
                    if (!foundShard) {  //assume no shards found for this chunk means the chunk doesn't exist
                        if (i == 0) {
                            System.out.println("File not found.");
                            return false;
                        }
                        break chunkloop;
                    }
                    byte[] chunk = decode(shards);
                    if (chunk == null)
                        return false;
                    if (needsRedispersed) {
                        System.out.println("Shard missing, redispersing " + chunkFilename + ".*");
                        disperseChunk(chunkFilename, chunk);
                    }
                    chunks.add(chunk);
                }
            }
            if (chunks.size() == Helper.readLimit) {
                System.out.println("File too large (maximum number of chunks exceeded)");
                return false;
            }

            dechunkifyList(fileName, chunks); // writes file to disk
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    private byte[] decode(ArrayList<byte[]> shardsIn) {
        int shardSize = 0;

        boolean[] shardsPresent = new boolean[TOTAL_SHARDS];

        for (int i = 0; i < shardsIn.size(); i++) {
            if (shardsIn.get(i) != null) {
                shardSize = shardsIn.get(i).length;
                shardsPresent[i] = true;
            }
        }
        byte[][] shardsOut = new byte[shardsIn.size()][shardSize];
        for (int i = 0; i < shardsOut.length; i++) {
            if (shardsIn.get(i) != null) {
                System.arraycopy(shardsIn.get(i), 0, shardsOut[i], 0, shardSize);
            }
        }
        return Helper.erasureDecode(shardsOut, shardsPresent, shardSize);
    }

    private byte[][][] chunkify(String fileName) {
        byte[] contents;
        try (FileInputStream fileInputStream = new FileInputStream(fileName)) {
            contents = fileInputStream.readAllBytes();
        } catch (IOException e) {
            return null;
        }
        int numChunks = contents.length / Helper.BpChunk;
        if (contents.length % Helper.BpChunk != 0) numChunks++;
        byte[][][] chunks = new byte[numChunks][1][Helper.BpChunk];
        for (int i = 0; i < numChunks; i++) {
            int startIdx = i*Helper.BpChunk;
            int endIdx = Integer.min((i*Helper.BpChunk)+Helper.BpChunk, contents.length);
            chunks[i][0] = Arrays.copyOfRange(contents, startIdx, endIdx);
        }
        return chunks;
    }

    private void dechunkifyList(String fileName, ArrayList<byte[]> chunks) throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(fileName);
        for (byte[] chunk : chunks) {
            fileOutputStream.write(chunk);
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
