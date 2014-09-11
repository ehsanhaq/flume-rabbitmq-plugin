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
import com.google.common.collect.Lists;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class RabbitMQConnectionFactory {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMQConnectionFactory.class);

    private final List<Address> hosts;
    private final ConnectionFactory connectionFactory;

    private RabbitMQConnectionFactory(List<Address> hosts, int port,
                                      String virtualHost, int connectionTimeout, int requestedHeartbeat,
                                      int requestedChannelMax, int requestedFrameMax, String userName,
                                      String password, ConnectionFactory connectionFactory) {

        Preconditions.checkArgument(hosts != null, "host can not be null");
        Preconditions.checkArgument(!hosts.isEmpty(), "hosts can not be empty");
        Preconditions.checkArgument(userName != null, "user name can not be null");
        Preconditions.checkArgument(password != null, "password can not be null");
        Preconditions.checkArgument(virtualHost != null,
                "virtual host can not be null");
        Preconditions.checkArgument(connectionFactory != null,
                "connection factory can not be null");

        this.hosts = hosts;

        this.connectionFactory = connectionFactory;
        connectionFactory.setPort(port);
        if (this.hosts.size() == 1) { // Only one host to connect
            Address address = hosts.get(0);
            this.connectionFactory.setHost(address.getHost());
            if (address.getPort() != -1) {  // If port is also supplied then use it.
                this.connectionFactory.setPort(address.getPort());
            }
        }
        this.connectionFactory.setVirtualHost(virtualHost);
        this.connectionFactory.setConnectionTimeout(connectionTimeout);
        this.connectionFactory.setRequestedHeartbeat(requestedHeartbeat);
        this.connectionFactory.setRequestedChannelMax(requestedChannelMax);
        this.connectionFactory.setRequestedFrameMax(requestedFrameMax);
        this.connectionFactory.setUsername(userName);
        this.connectionFactory.setPassword(password);
    }

    public Connection createConnection() throws IOException {
        Connection connection = null;
        if (this.hosts.size() == 1) {
            logger.info("Connecting to {}:{}...", connectionFactory.getHost(), connectionFactory.getPort());
            connection = connectionFactory.newConnection();
        } else {
            logger.info("Connecting to {}", hosts);
            connection = connectionFactory.newConnection(hosts.toArray(new Address[1]));
        }
        logger.info("Connection established.");
        return connection;
    }

    public static class Builder {
        private static ConnectionFactory connectionFactory = new ConnectionFactory();
        public static void setConnectionFactory(ConnectionFactory connFactory) {
             connectionFactory = connFactory;
        }

        private List<Address> hosts = Lists.newArrayList();
        private int port = ConnectionFactory.DEFAULT_AMQP_PORT;
        private String virtualHost = ConnectionFactory.DEFAULT_VHOST;
        private int connectionTimeout = ConnectionFactory.DEFAULT_CONNECTION_TIMEOUT;
        private int requestedHeartbeat = ConnectionFactory.DEFAULT_HEARTBEAT;
        private int requestedChannelMax = ConnectionFactory.DEFAULT_CHANNEL_MAX;
        private int requestedFrameMax = ConnectionFactory.DEFAULT_FRAME_MAX;
        private String userName = ConnectionFactory.DEFAULT_USER;
        private String password = ConnectionFactory.DEFAULT_PASS;

        public Builder addHost(String host) {
            Preconditions.checkArgument(host != null, "host string can not be null");
            this.hosts.add(Address.parseAddress(host));
            return this;
        }

        public Builder addHosts(String hosts) {
            Preconditions.checkArgument(hosts != null, "hosts string can not be null");
            Collections.addAll(this.hosts, Address.parseAddresses(hosts));
            return this;
        }

        public Builder setPort(int port) {
            this.port = port;
            return this;
        }

        public Builder setVirtualHost(String virtualHost) {
            this.virtualHost = virtualHost;
            return this;
        }

        public Builder setConnectionTimeOut(int connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
            return this;
        }

        public Builder setRequestedHeartbeat(int requestedHeartbeat) {
            this.requestedHeartbeat = requestedHeartbeat;
            return this;
        }

        public Builder setRequestedChannelMax(int requestedChannelMax) {
            this.requestedChannelMax = requestedChannelMax;
            return this;
        }

        public Builder setRequestedFrameMax(int requestedFrameMax) {
            this.requestedFrameMax = requestedFrameMax;
            return this;
        }

        public Builder setUserName(String userName) {
            this.userName = userName;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public RabbitMQConnectionFactory build() {
            if (hosts.isEmpty()) {
                hosts.add(Address.parseAddress(ConnectionFactory.DEFAULT_HOST));
            }
            return new RabbitMQConnectionFactory(hosts, port, virtualHost,
                    connectionTimeout, requestedHeartbeat, requestedChannelMax,
                    requestedFrameMax, userName, password, connectionFactory);
        }
    }

    ;
}
