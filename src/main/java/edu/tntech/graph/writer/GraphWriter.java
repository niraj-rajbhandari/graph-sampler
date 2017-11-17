package edu.tntech.graph.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.tntech.graph.enumerator.EdgeType;
import edu.tntech.graph.helper.ConfigReader;
import edu.tntech.graph.helper.GraphHelper;
import edu.tntech.graph.helper.Helper;
import edu.tntech.graph.pojo.Edge;
import edu.tntech.graph.pojo.GraphProperty;
import edu.tntech.graph.pojo.Node;
import edu.tntech.graph.pojo.Sample;
import edu.tntech.graph.sampler.Sampler;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.stream.Collectors;

public class GraphWriter {

    private static final String GRAPH_FILE_PROPERTY_KEY = "graph-file";
    private Helper helper;

    private Sample sample;


    public GraphWriter(boolean isStored) throws IOException {
        helper = Helper.getInstance();
        if (isStored) {
            sample = _readStoredSample();
        } else {
            sample = Sampler.getInstance().getSample();
        }

        System.out.println("Sampled Edges:" + sample.getSampleEdges());
    }

    public void write() throws IOException {
        System.out.println("write called");
        String graphFile = Helper.getInstance()
                .getAbsolutePath(ConfigReader.getInstance().getProperty(GRAPH_FILE_PROPERTY_KEY));
        try (RandomAccessFile fileStream = new RandomAccessFile(graphFile, "rw");
             FileChannel channel = fileStream.getChannel()) {
            channel.truncate(0); //empty the file first
            _writeGraph(channel);
        }
    }

    private void _writeGraph(FileChannel channel) throws IOException {
        Integer xpCount = 0;
        for (String xp : sample.getSampleNodes().keySet()) {
            String xpLine = _getXPLine((++xpCount).toString());
            _writeToFile(channel, xpLine);
            _writeNodeToFile(channel, xp);
            _writeEdgeToFile(channel, xp);
        }
    }

    private List<String> test(Edge edge) {
        List<String> test = new ArrayList<>();
        test.add(edge.getSource());
        test.add(edge.getTarget());
        return test;
    }

    private void _writeNodeToFile(FileChannel channel, String xp)
            throws IOException {

        List<Node> nodesInXp = new ArrayList<>(this.sample.getSampleNodes().get(xp).values());
        Integer nodeCount = 1;
        for (Node node : nodesInXp) {
            String oldNodeId = node.getId(); //get old node id
            node.setId(nodeCount.toString()); // set incremental node id
            _orderEdges(node, oldNodeId, xp);
            String nodeLine = _getNodeLine(node);
            _writeToFile(channel, nodeLine);
            nodeCount++;
        }
    }


    private void _orderEdges(Node newNode, String oldNodeId, String xp) {
        if (!oldNodeId.equals(newNode.getId()) && this.getSortedSampledEdgesForGraph(xp) != null) {

            Map<String, Edge> edgesForXp = this.getSortedSampledEdgesForGraph(xp).stream()
                    .map(e -> _updateEdge(e, oldNodeId, newNode))
                    .collect(Collectors.toMap(e -> e.getId(), e -> e));

            this._setSampledEdgesForGraph(xp, edgesForXp);
        }
    }

    private Edge _updateEdge(Edge edge, String oldNodeId, Node newNode) {
        if (edge.getSource().equals(oldNodeId)) {
            edge.setSource(newNode.getId());
            edge.setSourceVertex(newNode);
        } else if (edge.getTarget().equals(oldNodeId)) {
            edge.setTarget(newNode.getId());
            edge.setTargetVertex(newNode);
        }
        return edge;
    }

    private void _writeEdgeToFile(FileChannel channel, String xp) throws IOException {
//        System.out.println(xp);
//        System.out.println(this.sample.getSampleEdges());
        if (!this.sample.getSampleEdges().isEmpty() && this.sample.getSampleEdges().get(xp) != null)
            for (Edge edge : this.sample.getSampleEdges().get(xp).values()) {
                String edgeLine = _getEdgeLine(edge);
                _writeToFile(channel, edgeLine);
            }
    }

    private String _getNodeLine(Node node) {
        StringBuilder nodeLineBuilder = new StringBuilder();
        String comment = _getCommentString(node);
        if (comment != null) {
            nodeLineBuilder.append(comment);
        }

        nodeLineBuilder.append("v ");
        nodeLineBuilder.append(node.getId());
        nodeLineBuilder.append(" ");

        String label = GraphHelper.getGraphLabel(node);
        if (label != null) {
            nodeLineBuilder.append(label);
        }
        nodeLineBuilder.append("\n");

        return nodeLineBuilder.toString();
    }

    private String _getEdgeLine(Edge edge) {
        StringBuilder edgeLineBuilder = new StringBuilder();
        String comment = _getCommentString(edge);
        if (comment != null) {
            edgeLineBuilder.append(comment);
        }

        edgeLineBuilder.append(edge.isDirected() ? EdgeType.DIRECTED.getLabel() : EdgeType.UN_DIRECTED.getLabel());
        edgeLineBuilder.append(" ");
        edgeLineBuilder.append(edge.getSource());
        edgeLineBuilder.append(" ");
        edgeLineBuilder.append(edge.getTarget());

        String label = GraphHelper.getGraphLabel(edge);
        if (label != null) {
            edgeLineBuilder.append(" ");
            edgeLineBuilder.append(label);
        }

        edgeLineBuilder.append("\n");

        return edgeLineBuilder.toString();
    }

    private String _getXPLine(String xp) {
        StringBuilder xpLineBuilder = new StringBuilder();
        xpLineBuilder.append("XP # ");
        xpLineBuilder.append(xp);
        xpLineBuilder.append("\n");
        return xpLineBuilder.toString();
    }

    private void _writeToFile(FileChannel channel, String content)
            throws IOException {
        byte[] contentBytes = content.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(contentBytes.length);
        buffer.put(contentBytes);
        buffer.flip();
        channel.write(buffer);
    }

    private String _getCommentString(GraphProperty property) {
        String comment = GraphHelper.getGraphComment(property);
        if (comment != null) {
            StringBuilder commentBuilder = new StringBuilder("//");
            commentBuilder.append(comment);
            commentBuilder.append("\n");
            return commentBuilder.toString();
        }
        return null;
    }

    public void _setSampledEdgesForGraph(String graphId, Map<String, Edge> sampledEdgesForGraph) {
        if (this.sample.getSampleEdges().containsKey(graphId)) {
            this.sample.getSampleEdges().put(graphId, sampledEdgesForGraph);
        }
    }

//    /**
//     * Get sorted list of nodes for a graph
//     *
//     * @param graphId
//     * @return Sorted List of nodes for a graph
//     * @author Niraj Rajbhandari <nrajbhand42@students.tntech.com>
//     */
//    private List<Node> _getSortedSampledNodesForGraph(String graphId) {
//        if (this.sample.getSampleNodes().containsKey(graphId)) {
//            List<Node> sampledNodesForGraph = new ArrayList<>(this.sample.getSampleNodes().get(graphId));
//            Collections.sort(sampledNodesForGraph, Comparator.comparing(Node::getIdNumber));
//            return sampledNodesForGraph;
//        }
//        return null;
//    }

    public List<Edge> getSortedSampledEdgesForGraph(String graphId) {
        if (this.sample.getSampleEdges().containsKey(graphId)) {
            System.out.println(this.sample);
            List<Edge> sampledEdgesForGraph = new ArrayList<>(this.sample.getSampleEdges().get(graphId).values());

            Collections.sort(sampledEdgesForGraph, Comparator.comparing(Edge::getSourceId).thenComparing(Edge::getTargetId));
            return sampledEdgesForGraph;
        }
        return null;
    }

    private Sample _readStoredSample() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        File sampleFile = new File(helper.getAbsolutePath(Sample.SAMPLE_FILE));
        return mapper.readValue(sampleFile, Sample.class);
    }
}
