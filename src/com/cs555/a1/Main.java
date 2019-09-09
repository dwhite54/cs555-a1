package com.cs555.a1;

import com.cs555.a1.chunkserver.ChunkServer;
import com.cs555.a1.client.Client;
import com.cs555.a1.controller.Controller;
import org.w3c.dom.ranges.RangeException;

import java.io.IOException;
import java.util.List;

public class Main {

    private static Process[] Processes;

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length == 0 || args[0].equals("--help")) {
            printUsage();
            System.exit(0);
        }

        int controllerPort = 0;
        int chunkPort = 0;
        String controllerMachine = "";
        String[] chunkMachines = {};
        boolean isClient = false;
        boolean isController = false;
        boolean isChunkServer = false;
        for (int i = 0; i < args.length; i++) {
            try {
                switch (args[i]) {
                    case "--controller-port":
                        controllerPort = Integer.parseInt(args[i+1]);
                        i++;
                        break;
                    case "--controller-machine":
                        controllerMachine = args[i+1];
                        i++;
                        break;
                    case "--chunk-port":
                        chunkPort = Integer.parseInt(args[i+1]);
                        i++;
                        break;
                    case "--chunk-machines":
                        chunkMachines = args[i+1].split(",");
                        i++;
                        if (chunkMachines.length == 0)
                            throw new IllegalArgumentException("Error parsing chunk machine list");
                        break;
                    case "--mode":
                        switch (args[i+1]) {
                            case "client":
                                isClient = true;
                                break;
                            case "controller":
                                isController = true;
                                break;
                            case "chunkserver":
                                isChunkServer = true;
                        }
                        break;
                }
            } catch (RangeException | IllegalArgumentException e) {  //catch range and parsing errors
                System.out.println("Error parsing arguments");
                printUsage();
            }
        }

        if (isClient &&
                (controllerMachine.equals("") || controllerPort == 0 || chunkMachines.length == 0 || chunkPort == 0)) {
            System.out.println("Incomplete arguments provided");
            printUsage();
        } // ignore other cases, since those should be managed programmatically

        if (isClient) {
            //TODO check if chunk servers/controller running before starting!! (allow multiple clients)
            spawnProcesses(controllerPort, controllerMachine, chunkPort, chunkMachines);
            Client client = new Client(controllerPort, controllerMachine, chunkPort, chunkMachines);
            client.run();
        }
        else if (isController) { // we now know that this was a child process, configured to start a controller proc
            Controller controller = new Controller(controllerPort, controllerMachine);
            controller.run();
        } else if (isChunkServer) {
            ChunkServer chunkServer = new ChunkServer(controllerPort, controllerMachine, chunkPort, chunkMachines);
            chunkServer.run();
        }

        Close();
    }

    private static void spawnProcesses(int controllerPort, String controllerMachine, int chunkPort, String[] chunkMachines) {
        String jarPath = String.format("%s/cs555-a1.jar", System.getProperty("user.dir"));
        Processes = new Process[chunkMachines.length+1];

        for (int i = 0; i < chunkMachines.length; i++) {
            String cmdArgs = String.format("--mode chunkserver --chunk-port %s", chunkPort);
            StartProcess(chunkMachines[i], jarPath, cmdArgs, i);
        }

        String cmdArgs = String.format("--mode controller --controller-port %s", controllerPort);
        StartProcess(controllerMachine, jarPath, cmdArgs, chunkMachines.length);

        for (Process p : Processes) {
            if (!p.isAlive() && p.exitValue() == 255) {
                System.out.printf("Process %s closed with -1 exit value%n", p.toString());
                getProcessOutput(p);
                Close();
                System.exit(-1);

            }
        }
    }

    private static void getProcessOutput(Process p) {
        String input = "";
        String errors = "";
        try {
            input = new String(p.getInputStream().readAllBytes());
            errors = new String(p.getErrorStream().readAllBytes());
        } catch (IOException e) {
            System.out.println("Error occurred reading input and error streams:" + e.getMessage());
            Close();
            System.exit(-1);
        }
        System.out.printf("InputStream: %s%n", input);
        System.out.printf("ErrorStream: %s%n", errors);
    }

    private static void printUsage() {
        System.out.println("Options:");
        System.out.println("\t--mode: [client,chunkserver,controller], determines whether to run this process as " +
                "controller, chunk server, or client which starts controller and chunk servers if not detected).\n");
        System.out.println("\t--controller-port: port the controller will communicate with");
        System.out.println("\t--controller-machine: machine the controller will run on");
        System.out.println("\t--chunk-port: port the chunk servers will communicate with");
        System.out.println("\t--chunk-port: comma-delimited list of machines the chunk servers will run on");
    }

    private static void StartProcess(String machine, String path, String args, int pIdx) {
        String javaJREPath = System.getProperty("java.home");
        String javaCmd = String.format("%s/bin/java -jar %s %s", javaJREPath, path, args);
        List<String> cmd = List.of("ssh", machine, "-tt", javaCmd, String.format(">%s/%s-output.txt", System.getProperty("user.dir"), machine));
        ProcessBuilder b = new ProcessBuilder(cmd);
        try {
            Processes[pIdx] = b.start();
            System.out.printf("Started process %s using command %s%n", Processes[pIdx].toString(), cmd.toString());
        } catch (IOException e) {
            System.out.printf("Error starting chunk process on machine %s%n", machine);
            System.out.println(e.getMessage());
            Close();
            System.exit(-1);
        }
    }

    private static void Close() {
        System.out.println("Closing...");
        if (Processes != null) {
            for (Process p : Processes) {
                if (p != null) {
                    System.out.printf("Found running process %s now closing%n", p.toString());
                    getProcessOutput(p);
                    p.destroyForcibly();
                }
            }
        }
    }
}
