package edu.tntech.graph.sampler;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import edu.tntech.graph.enumerator.FileType;
import edu.tntech.graph.exception.SampleNotStoredException;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class Sampler {

    public static final String GRAPH_SAMPLER = "graph-sampler";
    public static final String DATA_TYPE_PROPERTY = "data-type";
    public static final String EMAIL_DATA_TYPE = "email";

    private Logger LOGGER;
    private static ConfigReader config;

    private Sample sample;

    private Integer sampleSize;

    private static Sampler instance;

    private Helper helper;

    private List<String> nodesNotToRemove;

    private Sampler(Integer sampleSize) throws FileNotFoundException {
        this.sampleSize = sampleSize;
        this.helper = Helper.getInstance();
        String dataType = config.getProperty(DATA_TYPE_PROPERTY);
        LOGGER = Helper.getLogger(Sampler.class.getName(), dataType);
        LOGGER.setLevel(helper.getLogLevel(config));
        this.setEdgesNotToRemove();
        setSample();
    }

    private void setEdgesNotToRemove() {
        String edgesNotToRemove = config.getProperty(Sample.EDGES_NOT_TO_REMOVE_KEY);
        this.nodesNotToRemove = Arrays.asList(edgesNotToRemove.split(",")).stream()
                .map(l -> l.replace(" ", "").toUpperCase())
                .collect(Collectors.toList());
    }

    public static Sampler getInstance() throws FileNotFoundException {
        if (instance == null) {
            config = ConfigReader.getInstance();
            int sampleSize = Integer.parseInt(config.getProperty("sample-size"));
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

    public void setSample() {
        LOGGER.info("Setting Sample");
        boolean sampleStored = Boolean.parseBoolean(config.getProperty(Sample.STORE_SAMPLE_INDEX));
        if (sampleStored) {
            try {
                Path path =
                        Paths.get(helper.getAbsolutePath(Sample.SAMPLE_FILE, config.getProperty(Helper.DATA_TYPE_KEY)
                                , FileType.DATA));
                if (Files.exists(path)) {
                    sample = GraphHelper.getStoredSample();
                } else {
                    throw new SampleNotStoredException("Sample is not stored");
                }
            } catch (IOException e) {
                LOGGER.severe(e.getMessage() + "\n Sample is Not stored");
                sample = new Sample();
            } catch (SampleNotStoredException e) {
                LOGGER.severe(e.getMessage());
                sample = new Sample();
            }
        } else if (sample == null) {
            sample = new Sample();
        }
    }

    public Map<String, Map<Integer, Node>> getSampleNodes() {
        return sample.getSampleNodes();
    }

    public Map<String, Map<Integer, Edge>> getSampleEdges() {
        return sample.getSampleEdges();
    }

    public Integer getTotalSampledNodeCount() {
        if (sample != null) {
            return this.getSampleNodes().values().stream().mapToInt(Map::size).sum();
        }

        return 0;

    }

    public Integer getTotalSampledEdgeCount() {
        if (sample != null)
            return this.getSampleEdges().values().stream().mapToInt(Map::size).sum();

        return 0;
    }

    public void resetSampledEdgeTypeCount() {
        this.sample.setSampledEdgeTypeCount(new HashMap<>());
    }

    /**
     * Creates a graph sample
     *
     * @param edge edge to be sampled
     * @param time timestamp
     * @author Niraj Rajbhandari <nrajbhand@students.tntech.edu>
     */
    public void createSampleGraphFromStream(Edge edge, Integer time) {
        if (this._areNodesInSample(edge) || (this.getTotalSampledNodeCount() < this.sampleSize)) {
            this._addSampleEdge(edge, false);
        } else if (this._isEdgeTypeSignificant(edge, time)) {
            this._replaceSampleEdge(edge, time);
        }
    }

    /**
     * Writes the content of the sample to a json file
     *
     * @throws IOException
     */
    public void writeToFile() throws IOException {
        LOGGER.info("Writing to the file");
        LOGGER.info("Sample Size:" + this.getTotalSampledNodeCount());
        JsonFactory factory = new JsonFactory();
        ObjectMapper mapper = new ObjectMapper(factory);
        File outputFile = new File(helper.getAbsolutePath(Sample.SAMPLE_FILE, config.getProperty(Helper.DATA_TYPE_KEY)
                , FileType.DATA));
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        mapper.writeValue(outputFile, sample);

        if (config.getProperty(DATA_TYPE_PROPERTY) == EMAIL_DATA_TYPE)
            sample.reset();
    }

    /**
     * Replaces sampled graph properties with new properties
     *
     * @param edge Replace with nodes related to the edge
     * @param time timestamp
     * @author Niraj Rajbhandari <nrajbhand42@students.tntech.edu>
     */
    private void _replaceSampleEdge(Edge edge, Integer time) {
//        System.out.println("Replacing sample edge");
        double edgeProbability = this._getEdgeProbability();

        double uniformRandomNumber = helper.getContinuousUniformRandomNumber(0, 1);
        String graphId = GraphHelper.getGraphId(edge);

//        System.out.println(
//                "Uniform Random Number: " + uniformRandomNumber + " <= " + edgeProbability + " : edge probability");
        if (uniformRandomNumber <= edgeProbability && !this.sample.sampleGraphContainsEdge(edge, graphId)) {
//          For sampling with replacing edges
//            Edge edgeToReplace = this._getRandomEdge();

//            if (this._removeSampleEdge(edgeToReplace)) {
//                this._addSampleEdge(edge, true);
//            }

//          For sampling with replacing nodes
            this._replaceSampleNode(this._getRandomNode());
            this._replaceSampleNode(this._getRandomNode());
            this._addSampleEdge(edge, true);
        }
//        System.out.println("Completed Replacing sample edge");
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
//        System.out.println("Adding sample edge");
        String graphId = GraphHelper.getGraphId(edge);
        boolean addEdge = graphId != null
                && (!isReplaced || (this._sampleGraphContainsNode(edge.getSourceVertex(), graphId)
                && this._sampleGraphContainsNode(edge.getTargetVertex(), graphId)));

        if (addEdge) {
            if (!this.sample.getSampleEdges().containsKey(graphId)) {
                this.sample.getSampleEdges().put(graphId, new HashMap<>());
            }

            if (!this.sample.sampleGraphContainsEdge(edge, graphId)) {
                Map<Integer, Edge> sampledEdges = this.sample.getSampleEdges().get(graphId);
                sampledEdges.put(edge.getIdNumber(), edge);
                if (!isReplaced) {
                    //Add nodes if the edge is not for replacement
                    this._addSampleNode(edge);
                }
            }
//            System.out.println("Completed Adding sample edge");
            return true;
        }
//        System.out.println("Completed Adding sample edge");
        return false;
    }

    /**
     * Adds  nodes of edge to the sample
     *
     * @param edge node to be added to the sample
     * @return boolean for successful addition of a node to the sample
     * @author Niraj Rajbhandari <nrajbhand42@students.tntech.edu>
     */
    private boolean _addSampleNode(Edge edge) {
        return this._addSampleNode(edge.getSourceVertex()) && this._addSampleNode(edge.getTargetVertex());
    }

    /**
     * Adds node to the sample
     *
     * @param node node to be added
     * @return
     */
    private boolean _addSampleNode(Node node) {
        String graphId = GraphHelper.getGraphId(node);
        if (graphId != null) {
            if (!this.sample.getSampleNodes().containsKey(graphId)) {
                this.sample.getSampleNodes().put(graphId, new HashMap<>());
            }

            Map<Integer, Node> sampledNodes = this.sample.getSampleNodes().get(graphId);
            sampledNodes.put(node.getIdNumber(), node);
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
    private boolean _replaceSampleNode(Node node) {
        if (node != null && this.sample.sampleGraphContainsNode(node, GraphHelper.getGraphId(node))) {
            this._removeEdgesWithNode(node);
            this.sample.getSampleNodes().get(GraphHelper.getGraphId(node)).remove(node);
            return this._addSampleNode(node);
        }
        return false;
    }

    /**
     * Removes nodes in an edge [called when sampling by removing edges than nodes]
     *
     * @param edge
     * @param graphId
     * @return
     */
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

    /**
     * Removes edge and related nodes [opposite of removing nodes and then edges]
     *
     * @param edge
     * @return
     */
    private boolean _removeSampleEdge(Edge edge) {
        String graphId = GraphHelper.getGraphId(edge);
        boolean status = false;
        if (this._removeNodesOfEdge(edge, graphId)) {
            Map<Integer, Edge> edgesForGraph = this.sample.getSampleEdges().get(graphId);

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



        /*To check how many edges are removed for each node removed*/
//        String label = GraphHelper.getGraphLabel(node).replace("\"", "");
//        if (!this.sample.getRemovedEdgeInfo().containsKey(label)) {
//            List<Integer> removedEdgeInfo = new ArrayList<>();
//            removedEdgeInfo.add(0);
//            removedEdgeInfo.add(0);
//            this.sample.getRemovedEdgeInfo().put(label, removedEdgeInfo);
//        }
//
//        Integer removedNodeCount = this.sample.getRemovedEdgeInfo().get(label).get(0);
//        Integer removedEdgeCount = this.sample.getRemovedEdgeInfo().get(label).get(1);
//
//        removedNodeCount++;
//        removedEdgeCount += this.sample.getEdgesWithNode(node, graphId).size();
//        List<Integer> newRemovedEdgeInfo = new ArrayList<>();
//        newRemovedEdgeInfo.add(removedNodeCount);
//        newRemovedEdgeInfo.add(removedEdgeCount);
//
//        this.sample.getRemovedEdgeInfo().put(label, newRemovedEdgeInfo);
        /*End of checking the number of edge removed for each node*/

        Map<Integer, Edge> removedEdgeList = this.sample.getEdgesWithoutNode(node, graphId);
        this.sample.getSampleEdges().put(graphId, removedEdgeList);
    }

    /**
     * Gets Random Edge to be replaced for sampling
     *
     * @return random Edge
     * @author Niraj Rajbhandari <nrajbhand42@students.tntech.edu>
     */
    private Edge _getRandomEdge() {
        Edge randomEdge = null;
        if (!this.sample.getSampleEdges().isEmpty()) {
            String randomGraphId;
            List<Edge> edgeForSelectedGraph;
            while (randomEdge == null) {
                randomGraphId = _getRandomGraphId();
                edgeForSelectedGraph = new ArrayList<>(this.sample.getSampleEdges().get(randomGraphId).values());
                if (!edgeForSelectedGraph.isEmpty()) {
                    Integer randomEdgeIndex = ThreadLocalRandom.current().nextInt(edgeForSelectedGraph.size());
                    randomEdge = edgeForSelectedGraph.get(randomEdgeIndex);
                }

            }
        }
        return randomEdge;
    }

    /**
     * Gets random node to be replaced
     *
     * @return random Node
     */
    private Node _getRandomNode() {
//        System.out.println("Getting random node");
        Node randomNode = null;
        String randomGraphId;
        List<Node> nodeForSelectedGraph;
        double uniformRandomNumber = helper.getContinuousUniformRandomNumber(0, 1);
        int countSearch = 0;
        while (randomNode == null ||
                this.nodesNotToRemove
                        .contains(GraphHelper.getGraphLabel(randomNode).replace("\"", "").toUpperCase())) {
            randomGraphId = _getRandomGraphId();
            nodeForSelectedGraph = new ArrayList<>(this.sample.getSampleNodes().get(randomGraphId).values());
            if (!nodeForSelectedGraph.isEmpty()) {
                Integer randomNodeIndex = ThreadLocalRandom.current().nextInt(nodeForSelectedGraph.size());
                randomNode = nodeForSelectedGraph.get(randomNodeIndex);
            }

            //higher probability for isolated nodes (bias towards nodes with low degree)
            if (randomNode != null && countSearch != 10) {
                int edgesWithNode = this.sample.getEdgesWithNode(randomNode, randomGraphId).size();
                int bias = (edgesWithNode > 0) ? edgesWithNode : 1;
                double nodeProbability = 1 / bias;

                randomNode = (nodeProbability > uniformRandomNumber) ? randomNode : null;

            }
            countSearch++;
        }
//        System.out.println("Completed Getting random node: " + randomNode.getIdNumber());
        return randomNode;

    }

    /**
     * Gets probability of edge being added to the sample
     *
     * @return
     */
    private double _getEdgeProbability() {
        try {
            int processedNodeCount = StreamProcessor.getInstance().getProcessedNodeCount();

            return this.sampleSize / (double) processedNodeCount;
        } catch (IOException e) {
            double reservoirSampleProbability = 1 / (double) this.getTotalSampledEdgeCount();
            double simpleProbability = this.getTotalSampledNodeCount() / (double) this.getTotalSampledEdgeCount();
            return Math.max(reservoirSampleProbability, simpleProbability);
        }
    }

    /**
     * Gets random graph id from which to get random graph property to be replaced for sampling
     *
     * @return
     */
    private String _getRandomGraphId() {
//        System.out.println("Getting random graphId");
        List<String> graphIdList = this.sample.getSampleNodes().keySet().stream().collect(Collectors.toList());
        int limit = graphIdList.size() == 0 ? 1 : graphIdList.size();

        int randomIndex = ThreadLocalRandom.current().nextInt(limit);


        String randomGraphIndex = graphIdList.get(randomIndex);
//        System.out.println("Completed Getting random graphId: " + randomGraphIndex);
        return randomGraphIndex;
    }

    private Boolean _areNodesInSample(Edge edge) {
        String graphId = GraphHelper.getGraphId(edge);

        return (this.sample.sampleGraphContainsNode(edge.getSourceVertex(), graphId)
                && this.sample.sampleGraphContainsNode(edge.getTargetVertex(), graphId));
    }

    /**
     * Counts edge being streamed
     *
     * @param edgeType
     * @param timeStep
     * @return
     */
    private Integer _countStreamedEdgeType(String edgeType, Integer timeStep) {
        if (this.sample.getSampledEdgeTypeCount() == null ||
                this.sample.getSampledEdgeTypeCount().isEmpty()) {
            this.resetSampledEdgeTypeCount();
        }

        if (!this.sample.getSampledEdgeTypeCount().containsKey(edgeType)) {
            this.sample.getSampledEdgeTypeCount().put(edgeType, 0);
        }

        Integer edgeTypeCount = this.sample.getSampledEdgeTypeCount().get(edgeType) + 1;
        this.sample.getSampledEdgeTypeCount().put(edgeType, edgeTypeCount);

        return this.sample.getSampledEdgeTypeCount().get(edgeType);
    }

    /**
     * Checks if edge is in the middle range of frequency
     *
     * @param edge
     * @param timeStep
     * @return
     */
    private boolean _isEdgeTypeSignificant(Edge edge, Integer timeStep) {

        String edgeType = GraphHelper.getGraphLabel(edge);
        Integer edgeTypeCount = this._countStreamedEdgeType(edgeType, timeStep);

        List<Integer> values = new ArrayList<>(this.sample.getSampledEdgeTypeCount().values());
        double mean = helper.getMean(values);

        double standardDeviation = helper.getStandardDeviation(values, mean);

        double deviationFromMean = Math.abs((edgeTypeCount - mean));

        String sourceVertex = GraphHelper.getGraphLabel(edge.getSourceVertex());
        String targetVertex = GraphHelper.getGraphLabel(edge.getTargetVertex());

        boolean edgeNotToRemove = this.nodesNotToRemove.contains(sourceVertex.toUpperCase())
                || this.nodesNotToRemove.contains(targetVertex.toUpperCase());

        return edgeNotToRemove || standardDeviation <= deviationFromMean;
    }
}
