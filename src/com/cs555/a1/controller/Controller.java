package com.cs555.a1.controller;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Controller {
    private AsynchronousServerSocketChannel serverChannel;
    private Future<AsynchronousSocketChannel> acceptResult;

    public Controller(int controllerPort, String controllerMachine) {
        try {
            serverChannel = AsynchronousServerSocketChannel.open();
            InetSocketAddress hostAddress = new InetSocketAddress(controllerMachine, controllerPort);
            serverChannel.bind(hostAddress);
            acceptResult = serverChannel.accept();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        try {
            AsynchronousSocketChannel clientChannel = acceptResult.get();
            if ((clientChannel != null) && (clientChannel.isOpen())) {
                ByteBuffer buffer = ByteBuffer.allocate(32);
                Future<Integer> readResult = clientChannel.read(buffer);

                // do some computation

                readResult.get();

                buffer.flip();
                String message = new String(buffer.array()).trim();
                buffer = ByteBuffer.wrap(new String(message).getBytes());
                Future<Integer> writeResult = clientChannel.write(buffer);

                // do some computation
                writeResult.get();
                buffer.clear();


                clientChannel.close();
                serverChannel.close();

            }
        } catch (InterruptedException | IOException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
