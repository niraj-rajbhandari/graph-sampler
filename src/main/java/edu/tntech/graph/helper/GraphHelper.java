package edu.tntech.graph.helper;

import edu.tntech.graph.pojo.Edge;
import edu.tntech.graph.pojo.GraphProperty;

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

}
