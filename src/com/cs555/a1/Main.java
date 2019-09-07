package com.cs555.a1;

import org.w3c.dom.ranges.RangeException;

import java.io.IOException;
import java.util.List;

public class Main {

    private static Process[] Processes;

    public static void main(String[] args) {
        if (args.length == 0 || args[0].equals("--help")) {
            printUsage();
            System.exit(0);
        }

        int controllerPort = 0;
        int chunkPort = 0;
        String controllerMachine = "";
        String[] chunkMachines = {};
        boolean isClient = true;
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
                    case "--is-client":
                        isClient = false;
                        break;
                    case "--is-controller":
                        isClient = false;
                        break;
                    case "--is-chunkserver":
                        isClient = false;
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

        if (isClient)
            spawnProcesses(controllerPort, chunkPort, controllerMachine, chunkMachines);
        else if (controllerPort != 0) { // we now know that this was a child process, configured to start a controller proc

        } else if (chunkPort != 0) {

        }


        Close();
        System.exit(0);
    }

    private static void spawnProcesses(int controllerPort, int chunkPort, String controllerMachine, String[] chunkMachines) {
        String jarPath = String.format("%s/cs555-a1.jar", System.getProperty("user.dir"));
        Processes = new Process[chunkMachines.length+1];

        for (int i = 0; i < chunkMachines.length; i++) {
            String cmdArgs = String.format("--noclient --chunk-port %s", chunkPort);
            StartProcess(chunkMachines[i], jarPath, cmdArgs, i);
        }

        String cmdArgs = String.format("--noclient --controller-port %s", controllerPort);
        StartProcess(controllerMachine, jarPath, cmdArgs, chunkMachines.length);

        for (Process p : Processes) {
            if (!p.isAlive() && p.exitValue() == 255) {
                System.out.printf("Process %s closed with -1 exit value%n", p.toString());
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
                System.out.printf("InputStream:%s%n", input);
                System.out.printf("ErrorStream:%s%n", errors);
                Close();
                System.exit(-1);
            }
        }
    }

    private static void printUsage() {
        System.out.println("Options:");
        System.out.println("\t--mode: determines whether to run this process as controller, chunk server, " +
                "or client, starting controller and chunk servers if not detected");
        System.out.println("\t--controller-port: port the controller will communicate with");
        System.out.println("\t--controller-machine: machine the controller will run on");
        System.out.println("\t--chunk-port: port the chunk servers will communicate with");
    }

    private static void StartProcess(String machine, String path, String args, int pIdx) {
        String javaCmd = String.format("java -jar %s %s", path, args);
        List<String> cmd = List.of("ssh", machine, "-t", javaCmd);
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
                    System.out.println("Found running process " + p.toString() + " now closing.");
                    p.destroy();
                }
            }
        }
    }
}
