/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flume.rabbitmq.source;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.rabbitmq.RabbitMQChannel;
import org.apache.flume.rabbitmq.RabbitMQConnectionFactory;
import org.apache.flume.rabbitmq.RabbitMQExchange;
import org.apache.flume.rabbitmq.RabbitMQQueue;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.flume.rabbitmq.RabbitMQConfigurationConstants.*;
import static org.apache.flume.rabbitmq.RabbitMQQueue.Builder;
import static org.apache.flume.rabbitmq.RabbitMQQueue.DeclareMode;

public class RabbitMQSource extends AbstractSource
        implements Configurable, PollableSource {
    private static final Logger logger = LoggerFactory
            .getLogger(RabbitMQSource.class);

    private String hosts;
    private int port;
    private String virtualHost;
    private String userName;
    private String password;
    private int connectionTimeout;
    private int requestedHeartbeat;
    private int requestedChannelMax;
    private int requestedFrameMax;
    private String exchangeName;
    private String queueName;
    private Map<QueueParameters, Boolean> queueParameters;
    private String[] topics;

    private int batchSize;

    private CounterGroup counterGroup;

    private RabbitMQConnectionFactory connectionFactory;
    private Connection connection = null;
    private RabbitMQChannel channel = null;
    private boolean isMoreMessagesOnServer;

    public RabbitMQSource() {
        counterGroup = new CounterGroup();
    }

    @Override
    public void configure(Context context) {
        hosts = context.getString(HOSTNAME, ConnectionFactory.DEFAULT_HOST);

        port = context.getInteger(PORT, ConnectionFactory.DEFAULT_AMQP_PORT);
        virtualHost = context.getString(VIRTUAL_HOST, ConnectionFactory.DEFAULT_VHOST);

        userName = context.getString(USERNAME, ConnectionFactory.DEFAULT_USER);

        password = context.getString(PASSWORD, ConnectionFactory.DEFAULT_PASS);

        connectionTimeout = context.getInteger(CONNECTION_TIMEOUT, ConnectionFactory.DEFAULT_CONNECTION_TIMEOUT);
        requestedHeartbeat = context.getInteger(REQUESTED_HEARTBEAT, ConnectionFactory.DEFAULT_HEARTBEAT);
        requestedChannelMax = context.getInteger(REQUESTED_CHANNEL_MAX, ConnectionFactory.DEFAULT_CHANNEL_MAX);
        requestedFrameMax = context.getInteger(REQUESTED_FRAMEL_MAX, ConnectionFactory.DEFAULT_FRAME_MAX);

        connectionFactory = new RabbitMQConnectionFactory.Builder()
                .addHosts(hosts)
                .setPort(port)
                .setVirtualHost(virtualHost)
                .setUserName(userName)
                .setPassword(password)
                .setConnectionTimeOut(connectionTimeout)
                .setRequestedHeartbeat(requestedHeartbeat)
                .setRequestedChannelMax(requestedChannelMax)
                .setRequestedFrameMax(requestedFrameMax)
                .build();

        exchangeName = context.getString(EXCHANGE_NAME, StringUtils.EMPTY);
        queueName = context.getString(QUEUE_NAME, StringUtils.EMPTY);
        Preconditions.checkArgument(StringUtils.isNotEmpty(exchangeName) || StringUtils.isNotEmpty(queueName),
                "Atleast exchange name or queue name must be defined.");

        topics = StringUtils.split(context.getString(TOPICS, StringUtils.EMPTY), ",");

        String queueParams = context.getString(QUEUE_PARAMETERS, StringUtils.EMPTY);
        queueParameters = initializeQueueParameters(queueParams);

        batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
    }

    /**
     * Parses Queue parameters
     *
     * @param queueParameters Queue parameters as key value pairs like
     *                        key1=value1?key2=value2?....
     * @return A Map<{@link QueueParameters},Boolean>
     */
    private Map<QueueParameters, Boolean> initializeQueueParameters(
            String queueParameters) {
        Map<String, String> keyValueQueueParams = Splitter.on('?')
                .omitEmptyStrings()
                .trimResults(CharMatcher.is('"')).trimResults()
                .withKeyValueSeparator("=")
                .split(queueParameters.toLowerCase());
        Map<QueueParameters, Boolean> parsed = DEFAULT_QUEUE_PARAMETERS;
        for (String key : keyValueQueueParams.keySet()) {
            parsed.put(QueueParameters.valueOf(key.toUpperCase()),
                    Boolean.valueOf(keyValueQueueParams.get(key)));
        }
        return parsed;
    }

    @Override
    public Status process() throws EventDeliveryException {
        try {
            if (connection == null) {
                connection = connectionFactory.createConnection();
                counterGroup.incrementAndGet(COUNTER_NEW_CONNECTION);
            }
            if (channel == null) {
                channel = new RabbitMQChannel.Builder(connection).build();
                counterGroup.incrementAndGet(COUNTER_NEW_CHANNEL);
                setupChannel();
            }
            List<Event> events = readEvents(batchSize);
            if (events.isEmpty()) {
                counterGroup.incrementAndGet(COUNTER_EMPTY_BATCH);
                return Status.BACKOFF;
            }
            counterGroup.incrementAndGet(COUNTER_SUCCESS_BATCH);
            getChannelProcessor().processEventBatch(events);
            sendAcks(events);
            return Status.READY;
        } catch (IOException e) {
            resetConnection();
            throw new EventDeliveryException("Exception raised while processing", e);
        }
    }

    private void sendAcks(List<Event> events) throws IOException {
        for (Event event : events) {
            long deliveryTag = Long.parseLong(
                    event.getHeaders().get(HEADER_DELIVERYTAG_KEY));
            channel.getChannel().basicAck(deliveryTag, false);
        }
    }

    /**
     * Setup the channel, by
     * <ul>
     *     <li>Creating a queue or testing a queue already exists</li>
     *     <li>Testing the exchange exists, if an exchangeName is given and then binding the queue
     *     with the exchange on the given topics</li>
     * </ul>
     * @throws IOException
     */
    private void setupChannel() throws IOException {
        DeclareMode declareMode;
        if (queueParameters.get(QueueParameters.CREATE)) {
            declareMode = DeclareMode.ACTIVE;
        } else {
            declareMode = DeclareMode.PASSIVE;
        }
        RabbitMQQueue rmqQueue = new Builder(channel.getChannel())
                .queueName(queueName)
                .declareMode(declareMode)
                .durable(queueParameters.get(QueueParameters.DURABLE))
                .exclusive(queueParameters.get(QueueParameters.EXCLUSIVE))
                .autoDelete(queueParameters.get(QueueParameters.AUTODELETE))
                .build();
        String createdQueueName = rmqQueue.getQueueName();
        if (StringUtils.isNotEmpty(exchangeName)) {
            new RabbitMQExchange.Builder(channel.getChannel(), exchangeName).build();
        }
        if (StringUtils.isNotEmpty(queueName)) {
            Preconditions.checkState(StringUtils.equals(queueName, createdQueueName),
                    String.format("Queuename created %s is different then the queue provided %s",
                            createdQueueName, queueName));
        }

        // Bind only when there is any topic or queue was a system generated queue.
        if(topics.length > 0) {
            channel.bindQueueWithExchange(createdQueueName, exchangeName, topics);
        }
//        } else if (StringUtils.isEmpty(queueName)) {
//            channel.bindQueueWithExchange(createdQueueName, exchangeName, new String[]{""});
//        }
        queueName = rmqQueue.getQueueName();
    }

    private void resetConnection() {
        logger.info("Resetting connection.");
        if (channel != null) {
            channel.abortChannel();
            channel = null;
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                logger.error("Failed to close the connection.", e);
            } finally {
                connection = null;
            }
        }
    }

    private List<Event> readEvents(int batchSize) throws IOException {
        List<Event> events = Lists.newLinkedList();
        for (int i = 0; i < batchSize; i++) {
            Event event = readEvent();
            if (event != null) {
                events.add(event);
                if (!isMoreMessagesOnServer) { // No more messages left on server.
                    break;
                }
            } else {  // No more messages.
                break;
            }
        }
        return events;
    }

    private Event readEvent() throws IOException {
        GetResponse response;
        response = channel.getChannel().basicGet(queueName, false);
        if (response == null) {
            logger.info("No event to read from MQ");
            counterGroup.incrementAndGet(COUNTER_EMPTY_MQ_GET);
            return null;
        }
        if (response.getEnvelope() != null) {
            logger.debug("Envelope: {}", response.getEnvelope());
        }
        if (response.getProps() != null && response.getProps().getHeaders() != null) {
            logger.debug("Header: {}", response.getProps().getHeaders().toString());
        }
        logger.debug("Message: {}", new String(response.getBody()));
        counterGroup.incrementAndGet(COUNTER_SUCCESS_MQ_GET);
        isMoreMessagesOnServer = (response.getMessageCount() > 0);
        Map<String, String> eventHeader = createEventHeader(response);
        Event event = EventBuilder.withBody(response.getBody(), eventHeader);
        return event;
    }

    private Map<String, String> createEventHeader(GetResponse response) {
        Map<String, String> eventHeader = Maps.newHashMap();
        Map<String, Object> mqHeader = Maps.newHashMap();
        if (response.getProps() != null && response.getProps().getHeaders() != null)
            mqHeader = response.getProps().getHeaders();
        for (Map.Entry<String, Object> entry : mqHeader.entrySet()) {
            eventHeader.put(entry.getKey(), entry.getValue().toString());
        }
        Long receivingTimestamp = System.currentTimeMillis();
        String timestamp = Joiner.on(",").useForNull("").join(eventHeader.get(HEADER_FLUME_TIMESTAMP_KEY),
                receivingTimestamp.toString());
        eventHeader.put(HEADER_FLUME_TIMESTAMP_KEY, timestamp);
        eventHeader.put(HEADER_DELIVERYTAG_KEY, Long.toString(response.getEnvelope().getDeliveryTag()));
        return eventHeader;
    }
}
