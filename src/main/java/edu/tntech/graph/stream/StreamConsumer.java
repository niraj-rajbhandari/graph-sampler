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
    private static final String QUEUE_CONNECTION_HOST_PROPERTY = "connection-host";
    private ConfigReader config;
    private Long newDeliveryTag;
    private Long oldDeliveryTag;
    private static StreamConsumer instance = null;
    private Connection connection;

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

        Channel channel = this._getChannel();

        while (true) {
            // Window Limit reached
            boolean windowSizeConsumed = newDeliveryTag % consumptionLimitPerWindow == 0;
            boolean canSampleWindow = ((newDeliveryTag.intValue() == Sampler.getInstance().getSampleSize() || count == 60)
                    && newDeliveryTag != oldDeliveryTag);

            if (!channel.isOpen()) {
                connection.close();
                break;
            } else if (canSampleWindow) {
                try {
                    StreamProcessor streamProcessor = StreamProcessor.getInstance();
                    CompletableFuture<Boolean> filterProcessedNode = CompletableFuture.supplyAsync(() -> streamProcessor.filterProcessedNodes(windowCount));
                    boolean storeSample = Boolean.parseBoolean(config.getProperty(Sample.STORE_SAMPLE_INDEX));
                    this._resetConsumedItems();
                    if (storeSample) {
                        Sampler.getInstance().writeToFile();
                    }
                    GraphWriter graphWriter = new GraphWriter(storeSample);
                    graphWriter.write();

                    while (!filterProcessedNode.isDone()) ;

                    channel.basicAck(newDeliveryTag, true);
                    oldDeliveryTag = newDeliveryTag;
                    windowCount++;
                    count = 0;
                } catch (Exception e) {
                    e.printStackTrace();
                    break;
                }
            }
            count++;
            Thread.currentThread().sleep(1000);
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
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(config.getProperty(QUEUE_CONNECTION_HOST_PROPERTY));
        connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.basicQos(Sampler.getInstance().getSampleSize(), true);
        channel.queueDeclare(messageQueue, false, false, false, null);
        channel.basicConsume(messageQueue, false, _getConsumer(channel));
        return channel;
    }
}
