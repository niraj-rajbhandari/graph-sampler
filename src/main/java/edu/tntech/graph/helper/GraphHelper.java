package edu.tntech.graph.helper;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.tntech.graph.enumerator.FileType;
import edu.tntech.graph.pojo.Edge;
import edu.tntech.graph.pojo.GraphProperty;
import edu.tntech.graph.pojo.Node;
import edu.tntech.graph.pojo.Sample;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

public class GraphHelper {
    public static final String GRAPH_ID_KEY = "graph_id";
    public static final String GRAPH_COMMENT_KEY = "comment";
    public static final String GRAPH_LABEL_KEY = "label";
    public static final String GRAPH_INDEX_KEY = "id";
    public static final String GRAPH_EDGE_SOURCE_KEY = "source";
    public static final String GRAPH_EDGE_TARGET_KEY = "target";

    public static String getGraphId(GraphProperty property) {
        return (property.getAttributes().containsKey(GRAPH_ID_KEY)) ?
                property.getAttributes().get(GRAPH_ID_KEY) : null;
    }

    public static String getGraphComment(GraphProperty property) {
        return (property.getAttributes().containsKey(GRAPH_COMMENT_KEY)) ?
                property.getAttributes().get(GRAPH_COMMENT_KEY) : null;
    }

    public static String getGraphLabel(GraphProperty property) {
        return (property.getAttributes().containsKey(GRAPH_LABEL_KEY)) ?
                "\"" + property.getAttributes().get(GRAPH_LABEL_KEY) + "\"" : null;
    }

    public static String getGraphIndex(GraphProperty property) {
        return (property.getAttributes().containsKey(GRAPH_INDEX_KEY)) ?
                property.getAttributes().get(GRAPH_INDEX_KEY) : null;
    }

    public static String getEdgeSourceAttribute(Edge edge) {
        return (edge.getAttributes().containsKey(GRAPH_EDGE_SOURCE_KEY)) ?
                edge.getAttributes().get(GRAPH_EDGE_SOURCE_KEY) : null;
    }

    public static String getEdgeTargetAttribute(Edge edge) {
        return (edge.getAttributes().containsKey(GRAPH_EDGE_TARGET_KEY)) ?
                edge.getAttributes().get(GRAPH_EDGE_TARGET_KEY) : null;
    }

    public static void setEdgeTargetAttribute(Edge edge, String targetId) {
        edge.getAttributes().put(GRAPH_EDGE_TARGET_KEY, targetId);
    }

    public static void setEdgeSourceAttribute(Edge edge, String sourceId) {
        edge.getAttributes().put(GRAPH_EDGE_SOURCE_KEY, sourceId);
    }

    public static Sample getStoredSample() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        File sampleFile = new File(Helper.getInstance().getAbsolutePath(Sample.SAMPLE_FILE, FileType.DATA));
        Sample sample = mapper.readValue(sampleFile, Sample.class);
        for (Map.Entry<String, Map<Integer, Edge>> entry : sample.getSampleEdges().entrySet()) {
            Map<Integer, Edge> edges = entry.getValue().entrySet().stream()
                    .map(e -> mapEdgeSourceTarget(e, sample, entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            sample.getSampleEdges().put(entry.getKey(), edges);
        }
        return sample;
    }

    private static Map.Entry<Integer, Edge> mapEdgeSourceTarget(Map.Entry<Integer, Edge> entry, Sample sample, String graphId) {
        Edge edge = entry.getValue();
        Node sourceVertex = sample.getSampledGraphNode(edge.getSource(), graphId);

        Node targetVertex = sample.getSampledGraphNode(edge.getTarget(), graphId);

        edge.setSourceVertex(sourceVertex);
        edge.setTargetVertex(targetVertex);
        entry.setValue(edge);
        return entry;
    }

}
