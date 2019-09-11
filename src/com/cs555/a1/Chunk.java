package com.cs555.a1;

import java.nio.file.Path;
import java.nio.file.Paths;

public class Chunk {
    public String filename = "";
    private String path = "";
    public int of = 0;
    public int version = 0;
    public String[] hashes = null;
    public boolean isNew = true;

    public String getFullPath(){
        if (!filename.equals("") && !path.equals(""))
            return Paths.get(filename, path).toAbsolutePath().toString();
        else
            return "";
    }
}
