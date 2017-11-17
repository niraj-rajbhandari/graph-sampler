package edu.tntech.graph.enumerator;

public enum EdgeType {
    DIRECTED("d"),
    UN_DIRECTED("u");

    private String label;

    private EdgeType(String label) {
        this.label = label;
    }

    public String getLabel() {
        return this.label;
    }
}
