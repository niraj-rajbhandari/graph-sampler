package edu.tntech.graph.io;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class FileParser {
    private String filePath;
    private List<String> file;
    private int lineToRead;

    public FileParser(String file) {
        this.setFilePath(file);
        this.lineToRead = 0;
    }

    public FileParser() {
        this.lineToRead = 0;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public void openFile() throws IllegalArgumentException {
        if (this.filePath == null) {
            throw new IllegalArgumentException("Please set the file path");
        }
        try {
            this.file = Files.lines(Paths.get(this.filePath)).collect(Collectors.toList());
        } catch (IOException e) {
            throw new InvalidPathException(this.filePath, "File not found");
        }
    }

    public boolean hasNext() {
        return this.file.size() > 0;
    }

    public String next() {
        if (this.hasNext()) {
            return this.file.remove(this.lineToRead++);
        } else {
            throw new IndexOutOfBoundsException("There are no lines to read next");
        }

    }

}
