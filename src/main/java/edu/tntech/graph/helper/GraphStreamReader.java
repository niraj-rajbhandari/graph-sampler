package edu.tntech.graph.helper;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import edu.tntech.graph.pojo.Edge;
import edu.tntech.graph.pojo.GraphProperty;
import edu.tntech.graph.pojo.Node;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class GraphStreamReader {

    private JsonNode rootNode;

    public GraphStreamReader(){

    }

    public GraphStreamReader(String streamedGraph) throws IOException {
//        this.setRootNode(streamedGraph);
    }

    public JsonNode getRootNode() {
        return rootNode;
    }

//    public void setRootNode(String streamedGraph) throws IOException {
//        JsonFactory factory = new JsonFactory();
//        ObjectMapper mapper = new ObjectMapper(factory);
//
//        rootNode = mapper.readTree(streamedGraph);
//    }
//
//    public GraphProperty readItem() throws InvalidObjectException {
//        return readItem(rootNode);
//    }
//
//    public List<GraphProperty> readCompleteStream() throws InvalidObjectException {
//        if (!rootNode.isArray()) {
//            throw new InvalidObjectException("Needs to pass complete graph stream i.e. list of vertex and edges");
//        }
//
//        ArrayNode streamItems = (ArrayNode) rootNode;
//        Iterator<JsonNode> iterator = streamItems.iterator();
//        List<GraphProperty> graphProperties = new ArrayList<>();
//        while (iterator.hasNext()) {
//            graphProperties.add(readItem(iterator.next()));
//        }
//
//        return graphProperties;
//    }
//
//    private GraphProperty readItem(JsonNode itemNode) throws InvalidObjectException {
//        Iterator<Map.Entry<String, JsonNode>> fieldsIterator = itemNode.fields();
//        ObjectMapper mapper = new ObjectMapper();
//        while (fieldsIterator.hasNext()) {
//            Map.Entry<String, JsonNode> field = fieldsIterator.next();
//            switch (field.getKey()) {
//                case Node.NODE_INDEX:
//                    return mapper.convertValue(field.getValue(), Node.class);
//                case Edge.EDGE_INDEX:
//                    return mapper.convertValue(field.getValue(), Edge.class);
//                default:
//                    throw new InvalidObjectException("The item object is not allowed:" + field.getValue().toString());
//            }
//        }
//        return null;
//    }
}
