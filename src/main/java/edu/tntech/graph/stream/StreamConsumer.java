package edu.tntech.graph.stream;

import com.rabbitmq.client.*;
import edu.tntech.graph.helper.ConfigReader;
import edu.tntech.graph.helper.Helper;
import edu.tntech.graph.pojo.Sample;
import edu.tntech.graph.sampler.Sampler;
import edu.tntech.graph.writer.GraphWriter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

public class StreamConsumer {

    private Logger LOGGER;

    private static final String CONSUMPTION_COMPLETED_MESSAGE = "done";
    private static final String PROCESSED_ITEM_SIZE_PROPERTY = "processed-item-size";
    private static final String QUEUE_CONNECTION_HOST_PROPERTY = "connection-host";
    private static final String QUEUE_USERNAME_PROPERTY = "rabbitmq-username";
    private static final String QUEUE_PASSWORD_PROPERTY = "rabbitmq-password";
    private static final String QUEUE_VIRTUAL_HOST_PROPERTY = "rabbitmq-vhost";
    private static final String GRAPH_EXCHANGE_PROPERTY = "graph-exchange";
    private static final String GBAD_EXCHANGE_PROPERTY = "gbad-exchange";
    private static final String WINDOW_TIME_PROPERTY = "window-time";
    private static final String GBAD_ALGO_PROPERTY = "gbad-algo";

    private static final String QUEUE_PREFIX = "graph-stream";
    private static final String GBAD_QUEUE_PREFIX = "gbad";

    private static final String MQ_HEARTBEAT_PROPERTY = "mq-heartbeat";

    private static final String COMPLETED = "completed";

    private static final String EXCHANGE_TYPE = "direct";
    private static final Integer SINGLE_GBAD_NOTIFICATION = 1;
    private static final Integer SECOND_IN_MILLIS = 1000;


    private boolean consumptionCompleted = false;
    private ConfigReader config;
    private Long newDeliveryTag;
    private Long oldDeliveryTag;
    private static StreamConsumer instance = null;
    private String dataType;

    private Connection streamConnection;

    private Connection gbadConnection;
    private Channel gbadRequestChannel;
    private Channel gbadResponseChannel;

    private Integer windowCount;
    private Integer consumptionLimitPerWindow;

    private String messageQueue;

    private boolean gbadRunning;

    private StreamConsumer() throws FileNotFoundException {

        config = ConfigReader.getInstance();
        dataType = config.getProperty(Sampler.DATA_TYPE_PROPERTY);

        LOGGER = Helper.getLogger(StreamConsumer.class.getName(), dataType);

        newDeliveryTag = 0l;
        oldDeliveryTag = 0l;
        windowCount = 0;
        gbadRunning = false;

        Float processedItemConfig = Float.parseFloat(config.getProperty(PROCESSED_ITEM_SIZE_PROPERTY));
        Double processWindowSize = Math.ceil(Sampler.getInstance().getSampleSize() * processedItemConfig);
        consumptionLimitPerWindow = processWindowSize.intValue();

        LOGGER.info("Consumption per window: " + consumptionLimitPerWindow);

        messageQueue = QUEUE_PREFIX + "-" + config.getProperty(GBAD_ALGO_PROPERTY) + "-" + dataType;

    }

    public Connection getStreamConnection() {
        return streamConnection;
    }

    public Channel getGbadRequestChannel() {
        return gbadRequestChannel;
    }

    public Channel getGbadResponseChannel() {
        return gbadResponseChannel;
    }

    public Logger getLogger() {
        return LOGGER;
    }

    public static StreamConsumer getInstance() throws IOException {
        if (instance == null) {
            instance = new StreamConsumer();
        }
        return instance;
    }

    /**
     * Consumes the message queue
     *
     * @throws IOException
     * @throws TimeoutException
     * @throws InterruptedException
     * @author Niraj Rajbhandari <nrajbhand42@students.tntech.edu>
     */
    public void consume() throws IOException, TimeoutException, InterruptedException {
        int count = 1;
        Integer windowTime = Integer.parseInt(config.getProperty(WINDOW_TIME_PROPERTY));
        Channel channel = this._getChannel();
        _setGBADChannel();
        while (true) {
            // Window Limit reached
            // TODO: Select the process size. How ? static value or something related to
            boolean windowSizeConsumed = newDeliveryTag % consumptionLimitPerWindow == 0;
            boolean canSampleWindow = ((windowSizeConsumed || count == 60)
                    && newDeliveryTag != oldDeliveryTag && !gbadRunning) || (!gbadRunning && consumptionCompleted);

            canSampleWindow = consumptionCompleted;
            if (count % 4 == 0 || canSampleWindow)
                LOGGER.info("Can sample window: " + canSampleWindow + "  | is gbad running: " + gbadRunning +
                        " | windowSizeConsumed: " + windowSizeConsumed + " | count: " + count + " | oldDeliveryTag: " +
                        oldDeliveryTag + " | newDeliveryTag: " + newDeliveryTag);

            if (canSampleWindow) {

                try {
                    StreamProcessor streamProcessor = StreamProcessor.getInstance();
                    CompletableFuture<Boolean> filterProcessedNode =
                            CompletableFuture.supplyAsync(() -> streamProcessor.filterProcessedNodes(windowCount));
                    boolean storeSample = Boolean.parseBoolean(config.getProperty(Sample.STORE_SAMPLE_INDEX));
                    this._resetConsumedItems();
                    if (storeSample) {
                        Sampler.getInstance().writeToFile();
                    }
                    GraphWriter graphWriter = new GraphWriter(storeSample, windowCount);
                    graphWriter.write();

                    while (!filterProcessedNode.isDone()) ;

                    //acknowledge receiving all the messages on the window
//                    channel.basicAck(newDeliveryTag, true);
                    oldDeliveryTag = newDeliveryTag;

                    /*Resetting the edge type count after defined timestep*/
                    int resetTimeStep = Integer.parseInt(config.getProperty(Sample.TIMESTEP_RESET_KEY));
                    if (windowCount % resetTimeStep == 0) {
                        Sampler.getInstance().resetSampledEdgeTypeCount();
                    }

                    windowCount++;
                    count = 0;
//                    _notifyGBADRunner(Long.toString(newDeliveryTag));

                    if (consumptionCompleted) {
//                        _notifyGBADRunner(CONSUMPTION_COMPLETED_MESSAGE);
                        channel.close();
                        if (!gbadRunning) {
                            gbadRequestChannel.close();
                            gbadResponseChannel.close();
                            streamConnection.close();
                            gbadConnection.close();
                        }
                        LOGGER.info("CPU RunTime: " + Helper.getCpuTime());
                        break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    channel.close();
                    streamConnection.close();
                    gbadConnection.close();
                    break;
                }
            }
            count++;
            Thread.currentThread().sleep(windowTime * SECOND_IN_MILLIS);
        }
    }


    /**
     * Resets the consumed edges info
     */
    private void _resetConsumedItems() throws IOException {
        StreamProcessor.getInstance().resetProcessing();
    }

    /**
     * Gets the consumer for the message queue
     *
     * @param channel Channel which is to be consumed by the consumer
     * @return Default Consumer
     * @author Niraj Rajbhandari <nrajbhand42@students.tntech.edu>
     */
    private Consumer _getConsumer(Channel channel) {
        return new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                String streamedItem = new String(body, StandardCharsets.UTF_8);
                newDeliveryTag = envelope.getDeliveryTag();
                if (streamedItem.equals(CONSUMPTION_COMPLETED_MESSAGE)) {
                    LOGGER.info("Consumption Completed: " + newDeliveryTag);
                    consumptionCompleted = true;
                } else {
                    try {
                        LOGGER.info("Reading Streamed Item: " + newDeliveryTag);
                        StreamProcessor processor = StreamProcessor.getInstance();
                        processor.readStreamedItem(streamedItem, windowCount, newDeliveryTag);
                        LOGGER.info("Completed Reading Streamed Item: " + newDeliveryTag);
                        channel.basicAck(newDeliveryTag, true);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        };
    }

    private Consumer _gbadResponseConsumer(Channel responseChannel) {
        return new DefaultConsumer(responseChannel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                try {
                    String streamedItem = new String(body, StandardCharsets.UTF_8);

                    if (streamedItem.equals(COMPLETED)) {
                        gbadRunning = false;
                        Helper.writeCPUTimeToFile(windowCount, dataType);
                        if (consumptionCompleted) {
                            StreamConsumer.getInstance().getGbadRequestChannel().close();
                            StreamConsumer.getInstance().getGbadResponseChannel().close();
                            StreamConsumer.getInstance().getStreamConnection().close();
                        }
                    }
                    responseChannel.basicAck(envelope.getDeliveryTag(), true);
                } catch (TimeoutException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    /**
     * Get Message queue channel
     *
     * @return Channel of Message Queue
     * @throws TimeoutException
     * @throws IOException
     */
    private Channel _getChannel() throws TimeoutException, IOException {
        String exchange = config.getProperty(GRAPH_EXCHANGE_PROPERTY);
        _setConnection();
        Channel channel = streamConnection.createChannel();
        channel.basicQos(consumptionLimitPerWindow);
        channel.exchangeDeclare(exchange, EXCHANGE_TYPE);
        channel.queueDeclare(messageQueue, false, false, false, null);
        channel.queueBind(messageQueue, exchange, messageQueue);
        channel.basicConsume(messageQueue, false, _getConsumer(channel));
        return channel;
    }

    private void _setConnection() throws TimeoutException, IOException {
        if (streamConnection == null) {
            int heartbeatInMinutes = Integer.parseInt(config.getProperty(MQ_HEARTBEAT_PROPERTY));
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUsername(config.getProperty(QUEUE_USERNAME_PROPERTY));
            factory.setPassword(config.getProperty(QUEUE_PASSWORD_PROPERTY));
            factory.setHost(config.getProperty(QUEUE_CONNECTION_HOST_PROPERTY));
            factory.setVirtualHost(config.getProperty(QUEUE_VIRTUAL_HOST_PROPERTY));
//            int heartbeat = SECOND_IN_MILLIS * heartbeatInMinutes;
            int heartbeat = heartbeatInMinutes;
            factory.setRequestedHeartbeat(heartbeat);
            streamConnection = factory.newConnection();
        }
    }

    private void _setGBADConnection() throws TimeoutException, IOException {
        if (gbadConnection == null) {
            ConnectionFactory factory = _getConnectionFactory();
            int heartbeat = SECOND_IN_MILLIS * Integer.parseInt(config.getProperty(WINDOW_TIME_PROPERTY));
            factory.setRequestedHeartbeat(heartbeat);
            gbadConnection = factory.newConnection();
        }
    }

    private ConnectionFactory _getConnectionFactory() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername(config.getProperty(QUEUE_USERNAME_PROPERTY));
        factory.setPassword(config.getProperty(QUEUE_PASSWORD_PROPERTY));
        factory.setHost(config.getProperty(QUEUE_CONNECTION_HOST_PROPERTY));
        factory.setVirtualHost(config.getProperty(QUEUE_VIRTUAL_HOST_PROPERTY));
        return factory;
    }

    private void _setGBADChannel() throws TimeoutException, IOException {
        _setGBADConnection();
        String exchange = config.getProperty(GBAD_EXCHANGE_PROPERTY);
        if (gbadRequestChannel == null) {
            String messageQueue = GBAD_QUEUE_PREFIX + "-" + config.getProperty(GBAD_ALGO_PROPERTY) + "-" + dataType;
            gbadRequestChannel = gbadConnection.createChannel();
            gbadRequestChannel.addShutdownListener(getShutDownListener());
            gbadRequestChannel.exchangeDeclare(exchange, EXCHANGE_TYPE);
            gbadRequestChannel.basicQos(SINGLE_GBAD_NOTIFICATION);
            gbadRequestChannel.queueDeclare(messageQueue, false, false, false, null);
            gbadRequestChannel.queueBind(messageQueue, exchange, messageQueue);
        }

        if (gbadResponseChannel == null) {
            String responseMessageQueue = GBAD_QUEUE_PREFIX + "-" + config.getProperty(GBAD_ALGO_PROPERTY) + "-" +
                    dataType + "-response";

            gbadResponseChannel = gbadConnection.createChannel();
            gbadResponseChannel.addShutdownListener(getShutDownListener());
            gbadResponseChannel.exchangeDeclare(exchange, EXCHANGE_TYPE);
            gbadResponseChannel.basicQos(SINGLE_GBAD_NOTIFICATION);
            gbadResponseChannel.queueDeclare(responseMessageQueue, false, false, false, null);
            gbadResponseChannel.queueBind(responseMessageQueue, exchange, responseMessageQueue);
            gbadResponseChannel.basicConsume(responseMessageQueue, _gbadResponseConsumer(gbadResponseChannel));
        }
    }

    private void _notifyGBADRunner(String message) throws IOException {
        LOGGER.info("notifying GBAD");
        String messageQueue = GBAD_QUEUE_PREFIX + "-" + config.getProperty(GBAD_ALGO_PROPERTY) + "-" + dataType;
        String exchange = config.getProperty(GBAD_EXCHANGE_PROPERTY);
        gbadRequestChannel.basicPublish(exchange, messageQueue, null, message.getBytes());
        gbadRunning = true;
    }

    private ShutdownListener getShutDownListener() {
        ShutdownListener shutdownListener = new ShutdownListener() {
            @Override
            public void shutdownCompleted(ShutdownSignalException cause) {
                if (cause.isInitiatedByApplication()) {
                    LOGGER.info("Shutdown is initiated by application. Ignoring it.");
                    LOGGER.info(cause.getMessage());
                } else {
                    LOGGER.severe("Shutdown is NOT initiated by application: " + cause.getMessage());
                    LOGGER.severe(cause.getMessage());
//                    boolean cliMode = Boolean.getBoolean(PropertiesConfig.CLI_MODE);
//                    if (cliMode) {
//                        System.exit(-3);
//                    }
                }
            }
        };
        return shutdownListener;
    }


}
