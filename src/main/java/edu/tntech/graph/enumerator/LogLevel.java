package edu.tntech.graph.enumerator;

public enum LOG_LEVEL {
    INFO("info"),
    ERROR("error"),
    DEBUG("debug");
    
    private String level;

    private LOG_LEVEL(String level) {
        this.level = level;
    }

    public String getlevel() {
        return this.level;
    }
}
