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

package org.apache.flume.rabbitmq;

import com.google.common.base.Preconditions;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RabbitMQChannel {
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQChannel.class);

    private Connection connection;
    private int channelNumber;
    private final Channel channel;

    private RabbitMQChannel(Connection connection, int channelNumber) throws IOException {
        this.connection = connection;
        this.channelNumber = channelNumber;
        this.channel = createChannel();
    }

    private Channel createChannel() throws IOException {
        Channel channel = null;
        if (channelNumber == -1) {
            logger.info("Creating channel with internally allocated number...");
            channel = connection.createChannel();
        } else {
            logger.info("Creating channel with channel number {}...", channelNumber);
            channel = connection.createChannel(channelNumber);
        }
        logger.info("Channel created.");
        return channel;
    }

    public void abortChannel() {
        try {
            logger.info("Closing Channel.");
            channel.abort();
        } catch (IOException e) {
            logger.error("Unable to close channel.", e);
        }
    }

    public void bindQueueWithExchange(String queueName, String exchangeName, String[] topics) throws IOException {
        Preconditions.checkArgument(queueName != null, "Queue name can not be null");
        Preconditions.checkArgument(exchangeName != null, "Exchange name can not be null");
        Preconditions.checkArgument(topics != null, "topics can not be null");

        for (String topic:topics) {
            channel.queueBind(queueName, exchangeName, topic);
        }
    }

    public Channel getChannel() {
        return channel;
    }

    public static class Builder {
        private final Connection connection;

        private int channelNumber = -1;

        public Builder(Connection connection) {
            Preconditions.checkArgument(connection != null,
                    "connection can not be null");
            this.connection = connection;
        }

        public Builder setChannelNumber(int channelNumber) {
            this.channelNumber = channelNumber;
            return this;
        }

        public RabbitMQChannel build() throws IOException {
            return new RabbitMQChannel(connection, channelNumber);
        }
    }
}
