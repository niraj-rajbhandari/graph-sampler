package edu.tntech.graph.sampler;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.tntech.graph.helper.ConfigReader;
import edu.tntech.graph.helper.GraphHelper;
import edu.tntech.graph.helper.Helper;
import edu.tntech.graph.pojo.Edge;
import edu.tntech.graph.pojo.Node;
import edu.tntech.graph.pojo.Sample;
import edu.tntech.graph.stream.StreamProcessor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class Sampler {

    private Sample sample;

    private Integer sampleSize;

    private static Sampler instance;

    private Helper helper;

    private Sampler(Integer sampleSize) {
        this.sampleSize = sampleSize;
        this.sample = new Sample();
        this.helper = Helper.getInstance();
    }

    public static Sampler getInstance() throws FileNotFoundException {
        if (instance == null) {
            int sampleSize = Integer.parseInt(ConfigReader.getInstance().getProperty("sample-size"));
            instance = new Sampler(sampleSize);
        }
        return instance;
    }

    public Integer getSampleSize() {
        return sampleSize;
    }

    public Sample getSample() {
        return sample;
    }

    public Map<String, Map<Integer, Node>> getSampleNodes() {
        return sample.getSampleNodes();
    }

    public Map<String, Map<String, Edge>> getSampleEdges() {
        return sample.getSampleEdges();
    }

    public Integer getTotalSampledNodeCount() {
        return this.getSampleNodes().values().stream().mapToInt(Map::size).sum();
    }

    public Integer getTotalSampledEdgeCount() {
        return this.getSampleEdges().values().stream().mapToInt(Map::size).sum();
    }

    /**
     * Creates a graph sample
     *
     * @param edge edge to be sampled
     * @param time timestamp
     * @author Niraj Rajbhandari <nrajbhand@students.tntech.edu>
     */
    public void createSampleGraphFromStream(Edge edge, Integer time) {
        if (this.getTotalSampledNodeCount() < this.sampleSize) {
            this._addSampleEdge(edge, false);
        } else {
            this._replaceSampleEdge(edge, time);
        }
    }

    public void writeToFile() throws IOException {
        JsonFactory factory = new JsonFactory();
        ObjectMapper mapper = new ObjectMapper(factory);
        File outputFile = new File(helper.getAbsolutePath(Sample.SAMPLE_FILE));
        System.out.println("Sample: " + sample);
        mapper.writeValue(outputFile, sample);
        sample.reset();
    }

    /**
     * Replaces a node in the sample
     *
     * @param edge Replace with nodes related to the edge
     * @param time timestamp
     * @author Niraj Rajbhandari <nrajbhand42@students.tntech.edu>
     */
    private void _replaceSampleEdge(Edge edge, Integer time) {
//        System.out.println("##################");
//        System.out.println("Edge to sample [" + edge.getSource() + "," + edge.getTarget() + "]");
        double edgeProbability = this._getEdgeProbability();
//        System.out.println("Probability of Edge: " + edgeProbability);
        double uniformRandomNumber = helper.getContinuousUniformRandomNumber(0, 1);
//        System.out.println("Uniform Random Number" + uniformRandomNumber);

        if (uniformRandomNumber <= edgeProbability) {

            Edge edgeToReplace = this._getRandomEdge();

            if (this._removeSampleEdge(edgeToReplace)) {
                this._addSampleEdge(edge, true);
            }
        }
//        System.out.println("##################");
    }

    /**
     * Checks if the graph contains the node
     *
     * @param node    node to be checked
     * @param graphId graph in which to check the node
     * @return boolean
     */
    private boolean _sampleGraphContainsNode(Node node, String graphId) {
        return this.sample.sampleGraphContainsNode(node, graphId);
    }

    /**
     * Add/Replace an edge from the sample
     *
     * @param edge       edge to be add or replaced
     * @param isReplaced flag to check if the edge is to be replaced or added
     * @return boolean for successful addition/replacement of edge
     * @author Niraj Rajbhandari <nrajbhand42@students.tntech.edu>
     */
    private boolean _addSampleEdge(Edge edge, boolean isReplaced) {
        String graphId = GraphHelper.getGraphId(edge);
        boolean addEdge = graphId != null
                && (!isReplaced || (this._sampleGraphContainsNode(edge.getSourceVertex(), graphId)
                && this._sampleGraphContainsNode(edge.getTargetVertex(), graphId)));
        if (addEdge) {
            if (!this.sample.getSampleEdges().containsKey(graphId)) {
                this.sample.getSampleEdges().put(graphId, new HashMap<>());
            }

            if (!this.sample.sampleGraphContainsEdge(edge, graphId)) {
                this.sample.getSampleEdges().get(graphId).put(edge.getId(), edge);
                this._addSampleNode(edge);
            }
            return true;
        }
        return false;
    }

    /**
     * Adds a node to the sample
     *
     * @param edge node to be added to the sample
     * @return boolean for successful addition of a node to the sample
     * @author Niraj Rajbhandari <nrajbhand42@students.tntech.edu>
     */
    private boolean _addSampleNode(Edge edge) {
        String graphId = GraphHelper.getGraphId(edge);
        if (graphId != null) {
            if (!this.sample.getSampleNodes().containsKey(graphId)) {
                this.sample.getSampleNodes().put(graphId, new HashMap<>());
            }
            Map<Integer, Node> sampledNodes = this.sample.getSampleNodes().get(graphId);
            sampledNodes.put(edge.getSourceVertex().getIdNumber(), edge.getSourceVertex());
            sampledNodes.put(edge.getTargetVertex().getIdNumber(), edge.getTargetVertex());
            this.sample.getSampleNodes().put(graphId, sampledNodes);

            return true;
        }
        return false;
    }

    /**
     * Removes a node from the sample
     *
     * @param node Node to be removed
     * @author Niraj Rajbhandari <nrajbhand42@students.tntech.edu>
     */
    private boolean _removeSampleNode(Node node) {
        String graphId = GraphHelper.getGraphId(node);

        this._removeEdgesWithNode(node);
        Node removedNode = this.sample.getSampleNodes().get(graphId).remove(node);
        return removedNode.equals(node);
    }

    private boolean _removeNodesOfEdge(Edge edge, String graphId) {

        boolean status = true;
        Node sourceVertex = edge.getSourceVertex();
        Node targetVertex = edge.getTargetVertex();

        Map<Integer, Node> nodesForGraph = this.sample.getSampleNodes().get(graphId);

        Node removedSourceNode = nodesForGraph.remove(sourceVertex.getIdNumber());
        Node removedTargetNode = nodesForGraph.remove(targetVertex.getIdNumber());

        if ((removedSourceNode != null && !removedSourceNode.equals(sourceVertex)) ||
                (removedTargetNode != null && !removedTargetNode.equals(targetVertex))) {
            nodesForGraph.put(sourceVertex.getIdNumber(), sourceVertex);
            nodesForGraph.put(targetVertex.getIdNumber(), targetVertex);
            status = false;
        }
        this.sample.getSampleNodes().put(graphId, nodesForGraph);
        return status;
    }

    private boolean _removeSampleEdge(Edge edge) {
        String graphId = GraphHelper.getGraphId(edge);
        boolean status = false;
        if (this._removeNodesOfEdge(edge, graphId)) {
            Map<String, Edge> edgesForGraph = this.sample.getSampleEdges().get(graphId);

            Edge removedEdge = edgesForGraph.remove(edge.getId());

            if (removedEdge.equals(edge)) {
                this.sample.getSampleEdges().put(graphId, edgesForGraph);
                status = true;
            }
        }

        return status;
    }

    /**
     * Removes edges containing the node
     *
     * @param node node that should occur in the edge to be removed
     * @author Niraj Rajbhandari <nrajbhand42@students.tntech.edu>
     */
    private void _removeEdgesWithNode(Node node) {
        String graphId = GraphHelper.getGraphId(node);
        Map<String, Edge> removedEdgeList = this.sample.getSampleEdges().get(graphId).entrySet().stream()
                .filter(e -> (!e.getValue().getTarget().equals(node.getId())
                        && !e.getValue().getSource().equals(node.getId())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        this.sample.getSampleEdges().put(graphId, removedEdgeList);
    }

    private List<String> test(Edge e) {
        List<String> test = new ArrayList<>();
        test.add(e.getSource());
        test.add(e.getTarget());
        test.add(GraphHelper.getGraphId(e));
        return test;
    }

    /**
     * Gets Random Node to be removed for sampling
     *
     * @return random Node
     * @author Niraj Rajbhandari <nrajbhand42@students.tntech.edu>
     */
    private Edge _getRandomEdge() {
        Edge randomEdge = null;
        while (randomEdge == null) {
            if (!this.sample.getSampleEdges().isEmpty()) {
                String randomGraphId = this.sample.getSampleEdges().keySet().stream()
                        .collect(Collectors.toList())
                        .get(ThreadLocalRandom.current().nextInt(this.sample.getSampleEdges().size()));
                List<Edge> nodeForSelectedGraph = new ArrayList<>(this.sample.getSampleEdges().get(randomGraphId).values());
                if (!nodeForSelectedGraph.isEmpty()) {
                    Integer randomNodeIndex = ThreadLocalRandom.current().nextInt(nodeForSelectedGraph.size());
                    randomEdge = nodeForSelectedGraph.get(randomNodeIndex);
                }
            }
        }
        return randomEdge;
    }

    private double _getEdgeProbability() {
        try {
            return this.sampleSize / (double) StreamProcessor.getInstance().getProcessedNodeCount();
        } catch (IOException e) {
            double reservoirSampleProbability = 1 / (double) this.getTotalSampledEdgeCount();
            double simpleProbability = this.getTotalSampledNodeCount() / (double) this.getTotalSampledEdgeCount();
            return Math.max(reservoirSampleProbability, simpleProbability);
        }
    }


}
