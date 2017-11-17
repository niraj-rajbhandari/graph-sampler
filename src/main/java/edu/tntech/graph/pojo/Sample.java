package edu.tntech.graph.pojo;

import edu.tntech.graph.helper.GraphHelper;

import java.util.HashMap;
import java.util.Map;

public class Sample {
    public static final String STORE_SAMPLE_INDEX = "store-sample";
    public static final String SAMPLE_FILE = "sample.json";

    private Map<String, Map<Integer, Node>> sampleNodes;
    private Map<String, Map<Integer,Edge>> sampleEdges;

    public Map<String, Map<Integer, Node>> getSampleNodes() {
        return sampleNodes;
    }

    public void setSampleNodes(Map<String, Map<Integer, Node>> sampleNodes) {
        this.sampleNodes = sampleNodes;
    }

    public Map<String, Map<Integer,Edge>> getSampleEdges() {
        return sampleEdges;
    }

    public void setSampleEdges(Map<String, Map<Integer,Edge>> sampleEdges) {
        this.sampleEdges = sampleEdges;
    }

    public Sample() {
       this.reset();
    }

    public boolean sampleGraphContainsNode(Node node, String graphId) {
        return this.sampleNodes.containsKey(graphId) && this.sampleNodes.get(graphId).containsKey(node.getId());

    }

    public boolean sampleGraphContainsEdge(Edge edge, String graphId){
        String edgeId = GraphHelper.getGraphIndex(edge);
        return this.sampleEdges.containsKey(graphId) && this.sampleEdges.get(graphId).containsKey(edgeId);
    }

    public void reset() {
        this.sampleEdges = new HashMap<>();
        this.sampleNodes = new HashMap<>();
    }

    @Override
    public String toString() {
        return "Sample{" +
                "sampleNodes=" + sampleNodes +
                '}';
    }
}
