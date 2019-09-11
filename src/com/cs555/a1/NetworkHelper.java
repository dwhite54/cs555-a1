package com.cs555.a1;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class NetworkHelper {
    public void Compose(DataOutputStream dos, byte[] message) throws IOException {
        dos.writeInt(message.length);
        dos.write(message);
    }

    public byte[] Decompose(DataInputStream dis) throws IOException {
        int size = dis.readInt();
        byte[] output = new byte[size];
        dis.readFully(output);
        return output;
    }
}
