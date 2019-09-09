package com.cs555.a1.controller;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Controller {
    private AsynchronousServerSocketChannel serverChannel;
    private Future<AsynchronousSocketChannel> acceptResult;
    private AsynchronousSocketChannel clientChannel;
    private ServerSocket ss;
    private boolean shutdown = false;
    public Controller(int controllerPort, String controllerMachine) throws IOException {
        // server is listening on port 5056
        ss = new ServerSocket(controllerPort);
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    Close();
                    mainThread.join();
                } catch (InterruptedException | IOException e) {
                    System.exit(-1);
                }
            }
        });


    }

    private void Close() throws IOException {
        ss.close();
        shutdown = true;
    }

    public void run() throws IOException, InterruptedException {
        // running infinite loop for getting
        // client request
        while (!shutdown)
        {
            Socket s = null;
            Thread.sleep(100);
            try
            {
                // socket object to receive incoming client requests
                s = ss.accept();

                System.out.println("A new client is connected : " + s);

                // obtaining input and out streams
                DataInputStream dis = new DataInputStream(s.getInputStream());
                DataOutputStream dos = new DataOutputStream(s.getOutputStream());

                System.out.println("Assigning new thread for this client");

                // create a new thread object
                Thread t = new ClientHandler(s, dis, dos);

                // Invoking the start() method
                t.start();

            }
            catch (Exception e){
                if (s != null){
                    s.close();
                }
                e.printStackTrace();
            }
        }
    }
}
// ClientHandler class
class ClientHandler extends Thread
{
    final DataInputStream dis;
    final DataOutputStream dos;
    final Socket s;

    // Constructor
    ClientHandler(Socket s, DataInputStream dis, DataOutputStream dos)
    {
        this.s = s;
        this.dis = dis;
        this.dos = dos;
    }

    @Override
    public void run()
    {
        String received;
        String toreturn;
        while (true)
        {
            try {
                received = dis.readUTF();
                String[] split = received.split(" ");
                switch (split[0]) {
                    case "write" :
                        toreturn = "List of available online servers with space.";
                        break;
                    case "read" :
                        toreturn = "Chunk server which contains this chunk";
                        break;
                    case "hminor" :
                        toreturn = "nothing";
                        break;
                    case "hmajor" :
                        toreturn = "nothing";
                        break;
                    default:
                        toreturn = "Invalid input";
                        break;
                }
                dos.writeUTF(toreturn);
            } catch (IOException e) {
                e.printStackTrace();
                break;
            }
        }

        try
        {
            this.dis.close();
            this.dos.close();
        } catch(IOException e){
            e.printStackTrace();
        }
    }
}