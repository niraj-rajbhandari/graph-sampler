package edu.tntech.graph.stream;

import com.rabbitmq.client.*;
import edu.tntech.graph.helper.ConfigReader;
import edu.tntech.graph.pojo.Sample;
import edu.tntech.graph.sampler.Sampler;
import edu.tntech.graph.writer.GraphWriter;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

public class StreamConsumer {

    private static final String PROCESSED_ITEM_SIZE_PROPERTY = "processed-item-size";
    private static final String MESSAGE_QUEUE_PROPERTY = "message-queue";
    private static final String GBAD_MESSAGE_QUEUE_PROPERTY = "gbad-message-queue";
    private static final String QUEUE_CONNECTION_HOST_PROPERTY = "connection-host";
    private static final String QUEUE_USERNAME_PROPERTY = "rabbitmq-username";
    private static final String QUEUE_PASSWORD_PROPERTY = "rabbitmq-password";
    private static final String QUEUE_VIRTUAL_HOST_PROPERTY = "rabbitmq-vhost";
    private static final String GRAPH_EXCHANGE_PROPERTY = "graph-exchange";
    private static final String GBAD_EXCHANGE_PROPERTY = "gbad-exchange";
    private static final String WINDOW_TIME_PROPERTY = "window-time";

    private static final String EXCHANGE_TYPE = "direct";
    private static final Integer SINGLE_GBAD_NOTIFICATION = 1;
    private static final Integer SECOND_IN_MILLIS = 1000;

    private ConfigReader config;
    private Long newDeliveryTag;
    private Long oldDeliveryTag;
    private static StreamConsumer instance = null;

    private Connection connection;
    private Channel gbadChannel;

    private Integer windowCount;
    private Integer consumptionLimitPerWindow;

    private StreamConsumer() throws FileNotFoundException {
        config = ConfigReader.getInstance();
        newDeliveryTag = 0l;
        oldDeliveryTag = 0l;
        windowCount = 0;

        Float processedItemConfig = Float.parseFloat(config.getProperty(PROCESSED_ITEM_SIZE_PROPERTY));
        Double processWindowSize = Math.ceil(Sampler.getInstance().getSampleSize() * processedItemConfig);
        consumptionLimitPerWindow = processWindowSize.intValue();

        System.out.println("Sample size:" + Sampler.getInstance().getSampleSize());
        System.out.println(consumptionLimitPerWindow + ": consumption limit per window");
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
        Integer count = 1;
        Integer windowTime = Integer.parseInt(config.getProperty(WINDOW_TIME_PROPERTY));
        Channel channel = this._getChannel();
        _setGBADChannel();
        while (true) {
            // Window Limit reached
            // TODO: Select the process size. How ? static value or something related to
            boolean windowSizeConsumed = newDeliveryTag % consumptionLimitPerWindow == 0;
            boolean canSampleWindow = ((windowSizeConsumed || count == 60)
                    && newDeliveryTag != oldDeliveryTag);
            if (!channel.isOpen()) {
                connection.close();
                break;
            } else if (canSampleWindow) {
                try {
//                    StreamProcessor streamProcessor = StreamProcessor.getInstance();
//                    CompletableFuture<Boolean> filterProcessedNode = CompletableFuture.supplyAsync(() -> streamProcessor.filterProcessedNodes(windowCount));
                    boolean storeSample = Boolean.parseBoolean(config.getProperty(Sample.STORE_SAMPLE_INDEX));
                    this._resetConsumedItems();
                    if (storeSample) {
                        Sampler.getInstance().writeToFile();
                    }
                    GraphWriter graphWriter = new GraphWriter(storeSample);
                    graphWriter.write();

//                    while (!filterProcessedNode.isDone()) ;

                    channel.basicAck(newDeliveryTag, true);
                    oldDeliveryTag = newDeliveryTag;
                    windowCount++;
                    count = 0;
                    _notifyGBADRunner(Long.toString(newDeliveryTag));
                } catch (Exception e) {
                    e.printStackTrace();
                    channel.close();
                    connection.close();
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
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                try {
                    StreamProcessor processor = StreamProcessor.getInstance();
                    newDeliveryTag = envelope.getDeliveryTag();
                    String streamedItem = new String(body, StandardCharsets.UTF_8);
                    processor.readStreamedItem(streamedItem, windowCount);
                } catch (Exception e) {
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
        String messageQueue = config.getProperty(MESSAGE_QUEUE_PROPERTY);
        String exchange = config.getProperty(GRAPH_EXCHANGE_PROPERTY);
        _setConnection();
        Channel channel = connection.createChannel();
        channel.basicQos(consumptionLimitPerWindow);
        channel.exchangeDeclare(exchange, EXCHANGE_TYPE);
        channel.queueDeclare(messageQueue, false, false, false, null);
        channel.queueBind(messageQueue, exchange, messageQueue);
        channel.basicConsume(messageQueue, false, _getConsumer(channel));
        return channel;
    }

    private void _setConnection() throws TimeoutException, IOException {
        if (connection == null) {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUsername(config.getProperty(QUEUE_USERNAME_PROPERTY));
            factory.setPassword(config.getProperty(QUEUE_PASSWORD_PROPERTY));
            factory.setHost(config.getProperty(QUEUE_CONNECTION_HOST_PROPERTY));
            factory.setVirtualHost(config.getProperty(QUEUE_VIRTUAL_HOST_PROPERTY));
            int oneHourHeartBeat = 600 * 61;
            factory.setRequestedHeartbeat(oneHourHeartBeat);
            connection = factory.newConnection();
        }

    }

    private void _setGBADChannel() throws TimeoutException, IOException {
        if (gbadChannel == null) {
            String messageQueue = config.getProperty(GBAD_MESSAGE_QUEUE_PROPERTY);
            _setConnection();
            String exchange = config.getProperty(GBAD_EXCHANGE_PROPERTY);
            gbadChannel = connection.createChannel();
            gbadChannel.exchangeDeclare(exchange, EXCHANGE_TYPE);
            gbadChannel.basicQos(SINGLE_GBAD_NOTIFICATION);
            gbadChannel.queueDeclare(messageQueue, false, false, false, null);
            gbadChannel.queueBind(messageQueue, exchange, messageQueue);
        }
    }

    private void _notifyGBADRunner(String message) throws IOException {
        String messageQueue = config.getProperty(GBAD_MESSAGE_QUEUE_PROPERTY);
        String exchange = config.getProperty(GBAD_EXCHANGE_PROPERTY);
        gbadChannel.basicPublish(exchange, messageQueue, null, message.getBytes());
    }
}
