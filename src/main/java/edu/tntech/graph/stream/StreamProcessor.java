package edu.tntech.graph.stream;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.tntech.graph.enumerator.GraphPropertyType;
import edu.tntech.graph.helper.GraphHelper;
import edu.tntech.graph.pojo.Edge;
import edu.tntech.graph.pojo.Node;
import edu.tntech.graph.sampler.Sampler;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class StreamProcessor {

    private static Logger log = Logger.getLogger(StreamProcessor.class.getName());

    private static StreamProcessor instance = null;

    private Map<String, List<Edge>> unprocessedEdges;
    private Map<String, Map<String, Node>> processedNodes;

    private Integer streamedItemCount;
    private ObjectMapper mapper;

    private Sampler sampler;


    public static StreamProcessor getInstance() throws IOException {
        if (instance == null) {
            instance = new StreamProcessor();
        }
        return instance;
    }

    private StreamProcessor() throws IOException {
        JsonFactory factory = new JsonFactory();
        mapper = new ObjectMapper(factory);
        unprocessedEdges = new HashMap<>();
        this.processedNodes = new HashMap<>();
        this.resetProcessing();
        this.sampler = Sampler.getInstance();
    }

    public Map<String, List<Edge>> getUnprocessedEdges() {
        return unprocessedEdges;
    }

    public void setUnprocessedEdges(Map<String, List<Edge>> unprocessedEdges) {
        this.unprocessedEdges = unprocessedEdges;
    }

    public Map<String, Map<String, Node>> getProcessedNodes() {
        return processedNodes;
    }

    public void setProcessedNodes(Map<String, Map<String, Node>> processedNodes) {
        this.processedNodes = processedNodes;
    }

    public Integer getStreamedItemCount() {
        return streamedItemCount;
    }

    public void setStreamedItemCount(Integer streamedItemCount) {
        this.streamedItemCount = streamedItemCount;
    }

    /**
     * Reads streamed items
     *
     * @param streamedItem
     * @param windowCount
     * @throws IOException
     */
    public void readStreamedItem(String streamedItem, Integer windowCount) throws IOException {
        StreamItemProcessor itemProcessor = new StreamItemProcessor();
        itemProcessor.setStreamedItem(streamedItem);
        if (itemProcessor.getStreamedItemType() == GraphPropertyType.NODE) {
            this._readStreamedNode(itemProcessor, windowCount);
        } else if (itemProcessor.getStreamedItemType() == GraphPropertyType.EDGE) {
            this._readStreamedEdge(itemProcessor, windowCount);
        }
        streamedItemCount++;
    }

    /**
     * Read Streamed node
     *
     * @param itemProcessor
     * @param windowCount
     */
    private void _readStreamedNode(StreamItemProcessor itemProcessor, Integer windowCount) {
        Node streamedNode = mapper.convertValue(itemProcessor.getStreamedItem(), Node.class);
        streamedNode.setTimeStep(windowCount);
        //setting actual node ID
        String nodeId = GraphHelper.getGraphIndex(streamedNode);
        String nodeType = GraphHelper.getGraphLabel(streamedNode);
        streamedNode.setId(nodeId);

        String graphId = GraphHelper.getGraphId(streamedNode);
        if (!this._isNodeProcessed(streamedNode, graphId, windowCount)) {
            this.processedNodes.get(graphId).put(streamedNode.getId(), streamedNode);
            this._processUnprocessedEdge(graphId, windowCount);
        }

    }

    /**
     * Read streamed edge
     *
     * @param itemProcessor
     * @param windowCount
     */
    private void _readStreamedEdge(StreamItemProcessor itemProcessor, Integer windowCount) {
        Edge streamedEdge = mapper.convertValue(itemProcessor.getStreamedItem(), Edge.class);
        streamedEdge.setId(GraphHelper.getGraphIndex(streamedEdge));
        streamedEdge.setTimeStep(windowCount);
        streamedEdge.setTarget(GraphHelper.getEdgeTargetAttribute(streamedEdge));
        streamedEdge.setSource(GraphHelper.getEdgeSourceAttribute(streamedEdge));
        this._processEdge(streamedEdge, windowCount);

    }

    /**
     * Process streamed edge
     *
     * @param edge
     * @param windowCount
     */
    private void _processEdge(Edge edge, Integer windowCount) {
        String graphId = GraphHelper.getGraphId(edge);

        if (_isEdgeAllowedToParse(edge, graphId)) {
            this._sampleEdge(edge, graphId, windowCount);
        } else {
            this._addUnprocessedEdge(graphId, edge);
        }
    }

    /**
     * Sample the streamed edge
     *
     * @param edge
     * @param graphId
     * @param windowCount
     */
    private void _sampleEdge(Edge edge, String graphId, Integer windowCount) {
        edge.setSourceVertex(processedNodes.get(graphId).get(edge.getSource()));
        edge.setTargetVertex(processedNodes.get(graphId).get(edge.getTarget()));
        this.sampler.createSampleGraphFromStream(edge, windowCount);
    }

    /**
     * Add unprocessed edge to list
     *
     * @param graphId
     * @param edge
     */
    private void _addUnprocessedEdge(String graphId, Edge edge) {
        if (!unprocessedEdges.containsKey(graphId)) {
            unprocessedEdges.put(graphId, new ArrayList<>());
        }
        unprocessedEdges.get(graphId).add(edge);
    }

    /**
     * Process the unprocessed edge
     *
     * @param graphId
     * @param windowCount
     */
    private void _processUnprocessedEdge(String graphId, Integer windowCount) {
        if (!this.unprocessedEdges.isEmpty() && this.unprocessedEdges.get(graphId) != null) {
            List<Edge> edgesToProcess = this.unprocessedEdges.get(graphId).stream()
                    .filter(e -> _isEdgeAllowedToParse(e, graphId))
                    .collect(Collectors.toList());

            for (Edge edgeToProcess : edgesToProcess) {
                this._sampleEdge(edgeToProcess, graphId, windowCount);
                this._removeUnprocessedEdgeList(edgeToProcess, graphId);
            }
        }
    }

    /**
     * Checks if edge is allowed to parse
     *
     * @param edge
     * @param graphId
     * @return
     */
    private Boolean _isEdgeAllowedToParse(Edge edge, String graphId) {
        return this.processedNodes.containsKey(graphId) && !processedNodes.get(graphId).isEmpty()
                && processedNodes.get(graphId).containsKey(edge.getSource())
                && processedNodes.get(graphId).containsKey(edge.getTarget());
    }

    public void resetProcessing() {
        this.streamedItemCount = 0;
    }

    /**
     * Removes unprocessed edge after processing
     *
     * @param edge
     * @param graphId
     */
    private void _removeUnprocessedEdgeList(Edge edge, String graphId) {
        this.unprocessedEdges.get(graphId).remove(edge);
        if (this.unprocessedEdges.get(graphId).isEmpty()) {
            this.unprocessedEdges.remove(graphId);
        }
    }

    /**
     * Filters processed nodes
     *
     * @param timeStep
     * @return
     */
    public Boolean filterProcessedNodes(int timeStep) {
        log.info("filterd process called");
        for (Map.Entry<String, Map<String, Node>> nodes : this.processedNodes.entrySet()) {
            Map<String, Node> graphNodes = nodes.getValue().entrySet().stream()
                    .filter(n -> filterUnprocessedEdge(n.getValue(), timeStep))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            this.processedNodes.put(nodes.getKey(), graphNodes);

        }

        return true;
    }

    /**
     * Filters unprocessed nodes
     *
     * @param node
     * @param timeStep
     * @return
     */
    public boolean filterUnprocessedEdge(Node node, int timeStep) {
        if ((timeStep - node.getTimeStep()) > 10) {
            String graphId = GraphHelper.getGraphId(node);
            List<Edge> unprocessedEdge = this.unprocessedEdges.get(graphId).stream()
                    .filter(e -> (!e.getSource().equals(node.getId()) && !e.getTarget().equals(node.getId())))
                    .collect(Collectors.toList());
            this.unprocessedEdges.put(graphId, unprocessedEdge);
            return false;
        }
        return true;
    }

    /**
     * get processed nodes count
     *
     * @return
     */
    public Integer getProcessedNodeCount() {
        return this.processedNodes.values().stream().mapToInt(Map::size).sum();
    }

    private boolean _isNodeProcessed(Node node, String graphId, Integer timeStep) {

        if (!this.processedNodes.containsKey(graphId)) {
            this.processedNodes.put(graphId, new HashMap<>());
        }
        if (this.processedNodes.get(graphId).containsKey(node.getId())) {
            //setting the latest time-step when the node appears
            node.setTimeStep(timeStep);
            return true;
        }
        return false;
    }

}


