package edu.tntech.graph.stream;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.tntech.graph.enumerator.GraphPropertyType;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class StreamItemProcessor {
    private JsonNode streamedItem;
    private ObjectMapper mapper;
    private GraphPropertyType streamedItemType;

    public StreamItemProcessor(){
        JsonFactory factory = new JsonFactory();
        mapper = new ObjectMapper(factory);
    }

    public JsonNode getStreamedItem() {
        return streamedItem;
    }

    public GraphPropertyType getStreamedItemType() {
        return streamedItemType;
    }

    public void setStreamedItemType(GraphPropertyType streamedItemType) {
        this.streamedItemType = streamedItemType;
    }

//    public void setStreamedItem(String streamedItem) throws IOException{
//        JsonNode rootNode = mapper.readTree(streamedItem);
//        Iterator<Map.Entry<String, JsonNode>> fieldsIterator = rootNode.fields();
//        int count = 1;
//        while(fieldsIterator.hasNext() && count ==1){
//            Map.Entry<String, JsonNode> field = fieldsIterator.next();
//            if(field.getKey().equals(GraphPropertyType.EDGE.getItemType()))
//                streamedItemType = GraphPropertyType.EDGE;
//            else
//                streamedItemType = GraphPropertyType.NODE;
//            this.streamedItem = field.getValue();
//            count++;
//        }
//    }

    public void setStreamedItem(String streamedItem) throws IOException{
        JsonNode rootNode = mapper.readTree(streamedItem);
        Iterator<Map.Entry<String, JsonNode>> fieldsIterator = rootNode.fields();
        while(fieldsIterator.hasNext()){
            if(rootNode.has("source")){
                streamedItemType = GraphPropertyType.EDGE;
            }
            else
                streamedItemType = GraphPropertyType.NODE;
            this.streamedItem = rootNode;
            break;
        }


    }
}
