package edu.tntech.graph.enumerator;

public enum LogLevel {
    INFO("info"),
    ERROR("error"),
    DEBUG("debug");

    private String level;

    private LogLevel(String level) {
        this.level = level;
    }

    public String getlevel() {
        return this.level;
    }
}
