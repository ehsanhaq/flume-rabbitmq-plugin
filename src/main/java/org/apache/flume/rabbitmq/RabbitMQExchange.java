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
import com.google.common.collect.Maps;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class RabbitMQExchange {
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQExchange.class);

    private final Channel channel;
    private final String exchangeName;

    private final String exchangeType;
    private final boolean isPassive;
    private final boolean isDurable;
    private final boolean isAutoDelete;
    private final boolean isInternal;
    private final Map<String, Object> arguments;

    private RabbitMQExchange(Channel channel, String exchangeName, String exchangeType,
                             boolean isPassive, boolean isDurable, boolean isAutoDelete, boolean isInternal,
                             Map<String, Object> arguments) throws IOException {
        this.channel = channel;
        this.exchangeName = exchangeName;
        this.exchangeType = exchangeType;
        this.isPassive = isPassive;
        this.isDurable = isDurable;
        this.isAutoDelete = isAutoDelete;
        this.isInternal = isInternal;
        this.arguments = arguments;
        declareExchange();
    }

    private void declareExchange() throws IOException {
        if (isPassive) {
            logger.info("Checking if exchange {} exists.", exchangeName);
            channel.exchangeDeclarePassive(exchangeName);
        } else {
            logger.info("Creating an exchange {}", exchangeName);
            channel.exchangeDeclare(exchangeName, exchangeType, isDurable, isAutoDelete, isInternal, arguments);
        }
    }

    public static class Builder {

        private final Channel channel;
        private final String exchangeName;

        private boolean isPassive = true;   // By default create a passive exchange
        private String exchangeType = "direct"; // The default exchange type
        private boolean isDurable = false; // By default not a durable exchange
        private boolean isAutoDelete = false; // By default a non-auto delete exchange
        private boolean isInternal = false; // By default not an internal exchange
        private Map<String, Object> arguments = Maps.newHashMap(); // By default an empty arguments

        public Builder(Channel channel, String exchangeName) {
            Preconditions.checkArgument(channel != null, "channel can not be null");
            Preconditions.checkArgument(exchangeName != null, "exchangeName can not be null");
            this.channel = channel;
            this.exchangeName = exchangeName;
        }

        public Builder exchangeType(String exchangeType) {
            this.exchangeType = exchangeType;
            return this;
        }
        public Builder passive(boolean isPassive) {
            this.isPassive = isPassive;
            return this;
        }
        public Builder durable(boolean isDurable) {
            this.isDurable = isDurable;
            return this;
        }
        public Builder autoDelete(boolean isAutoDelete) {
            this.isAutoDelete = isAutoDelete;
            return this;
        }
        public Builder internal(boolean isInternal) {
            this.isInternal = isInternal;
            return this;
        }
        public Builder arguments(Map<String, Object> arguments) {
            this.arguments = arguments;
            return this;
        }
        public RabbitMQExchange build() throws IOException {
            return new RabbitMQExchange(channel, exchangeName, exchangeType, isPassive, isDurable, isAutoDelete,
                    isInternal, arguments);

        }
    }
}
