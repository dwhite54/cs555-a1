package com.cs555.a1;

import org.w3c.dom.ranges.RangeException;

import java.io.IOException;

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
                    case "--noclient":
                        isClient = false;
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

        String mainPath = Main.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        Processes = new Process[chunkMachines.length+1];

        for (int i = 0; i < chunkMachines.length; i++) {
            String cmdArgs = String.format("--noclient --chunk-port %s", chunkPort);
            StartProcess(chunkMachines[i], mainPath, cmdArgs, i);
        }

        String cmdArgs = String.format("--noclient --controller-port %s", controllerPort);
        StartProcess(controllerMachine, mainPath, cmdArgs, chunkMachines.length);

        //TODO check that each process is started?
        //TODO create client object to talk to Controller and Chunk Servers
        //TODO check that ssh process logic is working
        //TODO fill out remaining actual assignment logic
    }

    private static void printUsage() {
        System.out.println("Options:");
        System.out.println("\t--controller-port: port the controller will communicate with");
        System.out.println("\t--controller-machine: machine the controller will run on");
        System.out.println("\t--chunk-port: port the chunk servers will communicate with");
        System.out.println("\t--chunk-machines: (comma-delimited) list of machines to start chunk servers on");
        //hidden arg: --noclient, which just starts chunk server or controller (otherwise running as client)
    }

    private static void StartProcess(String machine, String path, String args, int pIdx) {
        String cmd = String.format("ssh %s -t \"java -jar %s %s\"",
                machine, path, args);
        ProcessBuilder b = new ProcessBuilder(cmd);
        try {
            Processes[pIdx] = b.start();
        } catch (IOException e) {
            System.out.println("Error starting chunk process on machine " + machine);
            System.out.println(e.getMessage());
            Close();
            System.exit(-1);
        }
    }

    private static void Close() {
        System.out.println("Closing...");
        if (Processes != null) {
            for (Process p : Processes) {
                if (p != null)
                    p.destroy();
            }
        }
    }
}
