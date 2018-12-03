package edu.tntech.graph.writer;

import edu.tntech.graph.enumerator.EdgeType;
import edu.tntech.graph.enumerator.FileType;
import edu.tntech.graph.helper.ConfigReader;
import edu.tntech.graph.helper.GraphHelper;
import edu.tntech.graph.helper.Helper;
import edu.tntech.graph.pojo.Edge;
import edu.tntech.graph.pojo.GraphProperty;
import edu.tntech.graph.pojo.Node;
import edu.tntech.graph.pojo.Sample;
import edu.tntech.graph.sampler.Sampler;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class GraphWriter {
    private Logger log;
    private static final String GRAPH_FILE_PROPERTY_KEY = "graph-file";

    private Sample sample;

    private boolean differentGraphs;
    private Integer window;
    private String dataType;
    private ConfigReader configReader;

    public GraphWriter() throws IOException {
        configReader = ConfigReader.getInstance();
        this.dataType = configReader.getProperty(Helper.DATA_TYPE_KEY);
        log = Helper.getLogger(GraphWriter.class.getName(), dataType);

        this.differentGraphs = new Boolean(configReader.getProperty(Sample.DIFFERENT_GRAPH_KEY));
    }

    public GraphWriter(boolean isStored, Integer window) throws IOException {
        this();
        this.setSample(isStored);
        this.window = window;

    }

    public void setSample(boolean isStored) throws IOException {
        if (isStored) {
            log.log(Level.FINE, "Reading sample from stored file");
            sample = _readStoredSample();
        } else {
            log.log(Level.FINE, "Getting sample from sampler");
            sample = Sampler.getInstance().getSample();
        }
    }

    /**
     * Write graph to the file
     *
     * @throws IOException
     */
    public void write() throws IOException {
        log.log(Level.FINE, "Writing the graph to the file");

        String graphFileName = configReader.getProperty(GRAPH_FILE_PROPERTY_KEY);
        String graphDirectory = Helper.getInstance()
                .getAbsolutePath("", dataType, FileType.GRAPH);
        System.out.println("directory: " + graphDirectory);
        Helper.rename(graphFileName, graphDirectory, "graph-" + window + ".g");
        String graphFile = Helper.getInstance()
                .getAbsolutePath(graphFileName,
                        configReader.getProperty(Helper.DATA_TYPE_KEY), FileType.GRAPH);
        try (RandomAccessFile fileStream = new RandomAccessFile(graphFile, "rw");
             FileChannel channel = fileStream.getChannel()) {
            channel.truncate(0); //empty the file first
            _writeGraph(channel);
        }
//        System.out.println("Removed edge info:");
//        System.out.println("===================");
//        System.out.println((this.sample == null) + "Is Null Sample?");
//        System.out.println(this.sample.getRemovedEdgeInfo());

        _reset();


    }

    /**
     * Reset the graph writer
     *
     * @throws IOException
     */
    private void _reset() throws IOException {
        log.log(Level.FINE, "Resetting the graph writer sample");
        sample = null;

        // Setting the sample for Sampler from the file for next time window
        Sampler.getInstance().setSample();
    }

    /**
     * Writes the graph
     *
     * @param channel
     * @throws IOException
     */
    private void _writeGraph(FileChannel channel) throws IOException {
        Integer xpCount = 1;
        Integer nodeCount = 1;
        for (String xp : sample.getSampleNodes().keySet()) {
            if (xpCount == 1 || this.differentGraphs) {
                String xpLine = _getXPLine((xpCount).toString());
                _writeToFile(channel, xpLine);
                xpCount++;
            }

            if (this.differentGraphs)
                nodeCount = 1; //reset the nodeCount if different graphs

            nodeCount = _writeNodeToFile(channel, xp, nodeCount);
            _writeEdgeToFile(channel, xp);
        }
    }

    /**
     * Write each node to the graph file
     *
     * @param channel
     * @param xp
     * @throws IOException
     */
    private Integer _writeNodeToFile(FileChannel channel, String xp, Integer nodeCount)
            throws IOException {

        List<Node> nodesInXp = new ArrayList<>(this.sample.getSampleNodes().get(xp).values());
        for (Node node : nodesInXp) {
            String oldNodeId = node.getId(); //get old node id
            node.setId(nodeCount.toString()); // set incremental node id
            _orderEdges(node, oldNodeId, xp);
            String nodeLine = _getNodeLine(node);
            _writeToFile(channel, nodeLine);
            nodeCount++;
        }

        return nodeCount;
    }


    /**
     * Orders the node in incremental fashion
     *
     * @param newNode
     * @param oldNodeId
     * @param xp
     */
    private void _orderEdges(Node newNode, String oldNodeId, String xp) {
        if (!oldNodeId.equals(newNode.getId()) && !this.sample.getSampleEdges().isEmpty()
                && this.sample.getSampleEdges().get(xp) != null) {

            Map<Integer, Edge> edgesForXp = this.sample.getSampleEdges().get(xp).values().stream()
                    .map(e -> _updateEdge(e, oldNodeId, newNode))
                    .collect(Collectors.toMap(Edge::getIdNumber, e -> e));
            this._setSampledEdgesForGraph(xp, edgesForXp);
        }
    }

    /**
     * Updates the edges source and target with incremental node id
     *
     * @param edge
     * @param oldNodeId
     * @param newNode
     * @return
     */
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

    /**
     * Writes edge to the file
     *
     * @param channel
     * @param xp
     * @throws IOException
     */
    private void _writeEdgeToFile(FileChannel channel, String xp) throws IOException {
        if (!this.sample.getSampleEdges().isEmpty() && this.sample.getSampleEdges().get(xp) != null)
            for (Edge edge : this.sample.getSampleEdges().get(xp).values()) {
                if (this.sample.sampleGraphContainsNode(edge.getSourceVertex(), xp)
                        && this.sample.sampleGraphContainsNode(edge.getTargetVertex(), xp)) {
                    String edgeLine = _getEdgeLine(edge);
                    _writeToFile(channel, edgeLine);
                }
            }
    }

    /**
     * Gets the node line to be written to the file
     *
     * @param node
     * @return
     */
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

    /**
     * Gets the edge line to be written to the file
     *
     * @param edge
     * @return
     */
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

    /**
     * Gets the positive graph line
     *
     * @param xp
     * @return
     */
    private String _getXPLine(String xp) {
        StringBuilder xpLineBuilder = new StringBuilder();
        xpLineBuilder.append("XP # ");
        xpLineBuilder.append(xp);
        xpLineBuilder.append("\n");
        return xpLineBuilder.toString();
    }

    /**
     * Writes the content to the file
     *
     * @param channel
     * @param content
     * @throws IOException
     */
    private void _writeToFile(FileChannel channel, String content) throws IOException {
        byte[] contentBytes = content.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(contentBytes.length);
        buffer.put(contentBytes);
        buffer.flip();
        channel.write(buffer);
    }

    /**
     * Gets the comment string
     *
     * @param property
     * @return
     */
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

    /**
     * Sets the sample edges for graph
     *
     * @param graphId
     * @param sampledEdgesForGraph
     */
    public void _setSampledEdgesForGraph(String graphId, Map<Integer, Edge> sampledEdgesForGraph) {
        if (this.sample.getSampleEdges().containsKey(graphId)) {
            this.sample.getSampleEdges().put(graphId, sampledEdgesForGraph);
        }
    }

    /**
     * Reads the stored sample
     *
     * @return
     * @throws IOException
     */
    private Sample _readStoredSample() throws IOException {
        return GraphHelper.getStoredSample();
    }

//    private boolean _edgesNodeExists(Edge edge, String graphId) {
//        System.out.println("Edge:"+edge);
//        System.out.println("Graph Id:"+graphId);
//        if (!this.sample.sampleGraphContainsNode(edge.getTargetVertex(), graphId)) {
//
//            Node originalNode = _findOriginalNode(edge.getTargetVertex());
//            if (originalNode == null) {
//                return false;
//            }
//            edge.setTargetVertex(originalNode);
//        }
//
//        if (!this.sample.sampleGraphContainsNode(edge.getSourceVertex(), graphId)) {
//            System.out.println(edge);
//            Node originalNode = _findOriginalNode(edge.getSourceVertex());
//            if (originalNode == null) {
//                return false;
//            }
//            edge.setSourceVertex(originalNode);
//        }
//        return true;
//    }
//
//    private Node _findOriginalNode(Node changedNode) {
//        System.out.println("Changed Node:"+ changedNode+"  "+changedNode.getAttributes());
//        String originalNodeId = GraphHelper.getGraphIndex(changedNode);
//        String graphId = GraphHelper.getGraphId(changedNode);
//        Node originalNode = new Node();
//        originalNode.setId(originalNodeId);
//        originalNode.setAttributes(changedNode.getAttributes());
//        if (this.sample.sampleGraphContainsNode(originalNode, graphId)) {
//            return this.sample.getSampleNodes().get(graphId).get(originalNodeId);
//        }
//        return null;
//    }
}
