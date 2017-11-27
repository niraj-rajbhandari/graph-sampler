package edu.tntech.graph.pojo;

import java.util.List;

public class GraphStream {
    private List<GraphProperty> graphStream;

    public List<GraphProperty> getGraphStream() {
        return graphStream;
    }

    public void setGraphStream(List<GraphProperty> graphStream) {
        this.graphStream = graphStream;
    }

    @Override
    public String toString() {
        return "GraphStream{" +
                "graphStream=" + graphStream +
                '}';
    }
}
