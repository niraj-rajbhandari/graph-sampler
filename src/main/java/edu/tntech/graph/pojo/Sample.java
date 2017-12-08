package edu.tntech.graph.pojo;

import edu.tntech.graph.helper.GraphHelper;

import java.util.HashMap;
import java.util.Map;

public class Sample {
    public static final String STORE_SAMPLE_INDEX = "store-sample";
    public static final String SAMPLE_FILE = "sample.json";

    private Map<String, Map<Integer, Node>> sampleNodes;
    private Map<String, Map<Integer, Edge>> sampleEdges;

    public Map<String, Map<Integer, Node>> getSampleNodes() {
        return sampleNodes;
    }

    public void setSampleNodes(Map<String, Map<Integer, Node>> sampleNodes) {
        this.sampleNodes = sampleNodes;
    }

    public Map<String, Map<Integer, Edge>> getSampleEdges() {
        return sampleEdges;
    }

    public void setSampleEdges(Map<String, Map<Integer, Edge>> sampleEdges) {
        this.sampleEdges = sampleEdges;
    }

    public Sample() {
        this.reset();
    }

    public boolean sampleGraphContainsNode(Node node, String graphId) {
        Integer nodeId = Integer.parseInt(GraphHelper.getGraphIndex(node));
        boolean sampleNodeHasGraph = this.sampleNodes != null && this.sampleNodes.containsKey(graphId);
        boolean sampleNodeGraphHasNode = sampleNodeHasGraph && this.sampleNodes.get(graphId).containsKey(nodeId);
        return sampleNodeGraphHasNode;

    }

    public boolean sampleGraphContainsEdge(Edge edge, String graphId) {
        Integer edgeId = Integer.parseInt(GraphHelper.getGraphIndex(edge));
        return this.sampleEdges != null && this.sampleEdges.containsKey(graphId) && this.sampleEdges.get(graphId).containsKey(edgeId);
    }

    public Node getSampledGraphNode(String nodeId, String graphId) {
        Integer nodeIdNumber = Integer.parseInt(nodeId);
        if (this.getSampleNodes().containsKey(graphId) && this.getSampleNodes().get(graphId).containsKey(nodeIdNumber)) {
            return this.getSampleNodes().get(graphId).get(nodeIdNumber);
        } else {
            return null;
        }

    }

    public void reset() {
        this.sampleEdges = new HashMap<>();
        this.sampleNodes = new HashMap<>();
    }

    @Override
    public String toString() {
        return "Sample{" +
                "sampleNodes=" + sampleNodes.size() + ", " +
                "sampleEdges=" + sampleEdges.size() +
                '}';
    }
}
