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

import com.google.common.collect.Maps;

import java.util.Map;

public class RabbitMQConfigurationConstants {
    /**
     * RabbitMQ hostname .
     */
    public static final String HOSTNAME = "hostname";

    /**
     * RabbitMQ port.
     */
    public static final String PORT = "port";

    /**
     * RabbitMQ virtual host.
     */
    public static final String VIRTUAL_HOST = "virtualHost";

    /**
     * RabbitMQ user name.
     */
    public static final String USERNAME = "username";

    /**
     * RabbitMQ password.
     */
    public static final String PASSWORD = "password";

    /**
     * RabbitMQ Connection Timeout.
     */
    public static final String CONNECTION_TIMEOUT = "connectionTimeout";
    /**
     * RabbitMQ Requested Heartbeat.
     */
    public static final String REQUESTED_HEARTBEAT = "requestedHeartbeat";
    /**
     * RabbitMQ Requested Channel Max.
     */
    public static final String REQUESTED_CHANNEL_MAX = "requestedChannelMax";
    /**
     * RabbitMQ Requested Frame Max.
     */
    public static final String REQUESTED_FRAMEL_MAX = "requestedFrameMax";

    /**
     * RabbitMQ exchange name.
     */
    public static final String EXCHANGE_NAME = "exchangeName";

    /**
     * RabbitMQ queue name.
     */
    public static final String QUEUE_NAME = "queueName";

    /**
     * RabbitMQ topics as a comma sepaarated list.
     */
    public static final String TOPICS = "topics";

    /**
     * Queue parameters. Should be of the form param1=value1?param2=value2?...
     */
    public static final String QUEUE_PARAMETERS = "queueParameters";

    public enum QueueParameters {
        CREATE, DURABLE, AUTODELETE, EXCLUSIVE
    }

    public static final Map<QueueParameters, Boolean> DEFAULT_QUEUE_PARAMETERS =
            Maps.newEnumMap(QueueParameters.class);

    /** Initialize all queue parameters to false by default. */
    static {
        for (QueueParameters p : QueueParameters.values()) {
            DEFAULT_QUEUE_PARAMETERS.put(p, false);
        }
        DEFAULT_QUEUE_PARAMETERS.put(QueueParameters.CREATE, true);
    }

    /**
     * Queue  parameter separator
     */
    public static final Character QUEUE_PARAMETER_SEPARATOR = '?';

    /**
     * Batch size to read messages from MQ.
     */
    public static final String BATCH_SIZE = "rabbitmq.batchSize";
    public static final int DEFAULT_BATCH_SIZE = 100;

    /**
     * Header key for timestamp.
     */
    public static final String HEADER_FLUME_TIMESTAMP_KEY = "timestamp";
    public static final String HEADER_DELIVERYTAG_KEY = "deliveryTag";

    /**
     * RabbitMQ Flume Counters
     */
    public static final String COUNTER_NEW_CONNECTION = "rabbitmq.newConnection";
    public static final String COUNTER_NEW_CHANNEL = "rabbitmq.newChannel";
    public static final String COUNTER_EMPTY_MQ_GET = "rabbitmq.emptyGet";
    public static final String COUNTER_SUCCESS_MQ_GET = "rabbitmq.successGet";
    public static final String COUNTER_EMPTY_BATCH = "rabbitmq.emptyBatch";
    public static final String COUNTER_SUCCESS_BATCH = "rabbitmq.successBatch";
}
