package edu.tntech.graph;

import edu.tntech.graph.pojo.Edge;
import edu.tntech.graph.stream.StreamConsumer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

public class Main {

    private static final String GRAPH_FILE_OPTION = "-graph_file";
    private static final String GRAPH_FILE_SHORT_OPTION = "--g";
    private static final String GRAPH_FILE_INDEX = "graph_file";

    private static final Logger log = Logger.getLogger(Main.class.getName());

    public static void main(String... args) {
//        testSet();
        try {
//            testSet();
            StreamConsumer streamConsumer = StreamConsumer.getInstance();
            streamConsumer.consume();
            log.info("Consumption completed");
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void testSet() {
        Set<Edge> testSet = new HashSet<>();
        for(int i=0;i<3;i++){
            Edge edge = new Edge();
            edge.setId("1");
            edge.setSource("2");
            edge.setTarget("3");
            Map<String, String> attributes = new HashMap<>();
            attributes.put("label", "this is a label");
            attributes.put("id", "1");
            attributes.put("source","2");
            attributes.put("target","3");
            attributes.put("graph_id","1");
            edge.setAttributes(attributes);

            testSet.add(edge);
        }


        System.out.println(testSet);
    }


    /***
     * Parses command line arguments
     * @param args array of command line arguments
     * @return Map of options to values
     */
    private static Map<String, String> parseArgs(String... args) {
        Map<String, String> arguments = new HashMap<>();
        if (args.length > 0) {
            for (int i = 0; i < args.length; i++) {
                String argument = args[i];
                if (argument.equals(GRAPH_FILE_OPTION) || argument.equals(GRAPH_FILE_SHORT_OPTION)) {
                    arguments.put(GRAPH_FILE_INDEX, args[++i]);
                }
                i++;
            }
        }
        return arguments;
    }

    /**
     * Displays usage of the application
     */
    private static void usage() {
        System.out.println("Sample graph stream");
        System.out.println("====================\n");

        System.out.print("Usage : ");
        System.out.println("========");
        System.out.println("./graph-sampler [options]");
        System.out.println("Options:");
        System.out.println("--------");
        System.out.println("\t-graph_file | --g [path/to/the.graph/to/be/sampled]");
        System.out.println("\tEx: ./graph-sampler --g /Users/resources/hydro.g");
    }
}
