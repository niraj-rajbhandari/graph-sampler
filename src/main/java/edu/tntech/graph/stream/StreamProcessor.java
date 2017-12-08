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

    private Map<String, List<Edge>> unprocessedEdgeList;
    private Map<String, Map<String, Node>> processedNodeList;

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
        unprocessedEdgeList = new HashMap<>();
        this.processedNodeList = new HashMap<>();
        this.resetProcessing();
        this.sampler = Sampler.getInstance();
    }

    public Map<String, List<Edge>> getUnprocessedEdgeList() {
        return unprocessedEdgeList;
    }

    public void setUnprocessedEdgeList(Map<String, List<Edge>> unprocessedEdgeList) {
        this.unprocessedEdgeList = unprocessedEdgeList;
    }

    public Map<String, Map<String, Node>> getProcessedNodeList() {
        return processedNodeList;
    }

    public void setProcessedNodeList(Map<String, Map<String, Node>> processedNodeList) {
        this.processedNodeList = processedNodeList;
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
        streamedNode.setId(nodeId);

        String graphId = GraphHelper.getGraphId(streamedNode);
        if (!this._isNodeProcessed(streamedNode, graphId, windowCount)) {
            this.processedNodeList.get(graphId).put(streamedNode.getId(), streamedNode);
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
        edge.setSourceVertex(processedNodeList.get(graphId).get(edge.getSource()));
        edge.setTargetVertex(processedNodeList.get(graphId).get(edge.getTarget()));
        this.sampler.createSampleGraphFromStream(edge, windowCount);
    }

    /**
     * Add unprocessed edge to list
     *
     * @param graphId
     * @param edge
     */
    private void _addUnprocessedEdge(String graphId, Edge edge) {
        if (!unprocessedEdgeList.containsKey(graphId)) {
            unprocessedEdgeList.put(graphId, new ArrayList<>());
        }
        unprocessedEdgeList.get(graphId).add(edge);
    }

    /**
     * Process the unprocessed edge
     *
     * @param graphId
     * @param windowCount
     */
    private void _processUnprocessedEdge(String graphId, Integer windowCount) {
        if (!this.unprocessedEdgeList.isEmpty() && this.unprocessedEdgeList.get(graphId) != null) {
            List<Edge> edgesToProcess = this.unprocessedEdgeList.get(graphId).stream()
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
        return this.processedNodeList.containsKey(graphId) && !processedNodeList.get(graphId).isEmpty()
                && processedNodeList.get(graphId).containsKey(edge.getSource())
                && processedNodeList.get(graphId).containsKey(edge.getTarget());
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
        this.unprocessedEdgeList.get(graphId).remove(edge);
        if (this.unprocessedEdgeList.get(graphId).isEmpty()) {
            this.unprocessedEdgeList.remove(graphId);
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
        for (Map.Entry<String, Map<String, Node>> nodes : this.processedNodeList.entrySet()) {
            Map<String, Node> graphNodes = nodes.getValue().entrySet().stream()
                    .filter(n -> filterUnprocessedEdge(n.getValue(), timeStep))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            this.processedNodeList.put(nodes.getKey(), graphNodes);

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
            List<Edge> unprocessedEdge = this.unprocessedEdgeList.get(graphId).stream()
                    .filter(e -> (!e.getSource().equals(node.getId()) && !e.getTarget().equals(node.getId())))
                    .collect(Collectors.toList());
            this.unprocessedEdgeList.put(graphId, unprocessedEdge);
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
        return this.processedNodeList.values().stream().mapToInt(Map::size).sum();
    }

    private boolean _isNodeProcessed(Node node, String graphId, Integer timeStep) {

        if (!this.processedNodeList.containsKey(graphId)) {
            this.processedNodeList.put(graphId, new HashMap<>());
        }
        if (this.processedNodeList.get(graphId).containsKey(node.getId())) {
            //setting the latest time-step when the node appears
            node.setTimeStep(timeStep);
            return true;
        }
        return false;
    }

}


