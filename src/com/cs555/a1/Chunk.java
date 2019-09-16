package com.cs555.a1;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;

public class Chunk {
    public String fileName = "";
    public int version = 0;
    public boolean isNew = true;
    public int sequence;
}