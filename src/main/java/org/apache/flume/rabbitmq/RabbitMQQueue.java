package org.apache.flume.rabbitmq;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.rabbitmq.client.Channel;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class RabbitMQQueue {
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQQueue.class);

    public enum DeclareMode {
        PASSIVE,          // only connects with an existing queue.
        ACTIVE            // create an idempotent queue.
    }
    private final Channel channel;

    private String queueName;
    private final DeclareMode declareMode;
    private final boolean isDurable;
    private final boolean isAutoDelete;
    private final boolean isExclusive;
    private final Map<String, Object> arguments;

    private RabbitMQQueue(Channel channel, String queueName, DeclareMode declareMode, boolean isDurable,
                          boolean isAutoDelete, boolean isExclusive,
                          Map<String, Object> arguments) throws IOException {
        this.channel = channel;
        this.queueName = queueName;
        this.declareMode = declareMode;
        this.isDurable = isDurable;
        this.isAutoDelete = isAutoDelete;
        this.isExclusive = isExclusive;
        this.arguments = arguments;
        declareQueue();
    }

    private void declareQueue() throws IOException {
        if (declareMode == DeclareMode.PASSIVE) {
            declarePassiveQueue(queueName);
        } else if (declareMode == DeclareMode.ACTIVE) {
            declareActiveQueue(queueName);
        }
    }
    private void declareActiveQueue(String queueName) throws IOException {
        if (StringUtils.isEmpty(queueName)) { // have server create a default queue
            // Create the server queue and set the queueName
            this.queueName = channel.queueDeclare().getQueue();
        } else { // create an explicit queue with the given name.
            channel.queueDeclare(queueName, isDurable, isExclusive, isAutoDelete, arguments);
        }
    }
    private void declarePassiveQueue(String queueName) throws IOException {
        Preconditions.checkArgument(StringUtils.isNotEmpty(queueName),
                "Queue name can not be null for declaring a passive queue");
        channel.queueDeclarePassive(queueName);
    }

    public String getQueueName() {
        return queueName;
    }

    public static class Builder {
        private final Channel channel;

        private String queueName = null; // By default no queue name is given for creating a server side queue.
        private DeclareMode declareMode = DeclareMode.ACTIVE; // By default create an idempotent queue.
        private boolean isDurable = false; // By default the queue is not durable
        private boolean isAutoDelete = true; // By default create an auto-delete queue.
        private boolean isExclusive = true; // By default create an exclusive queue.
        private Map<String, Object> arguments = Maps.newHashMap();

        public Builder(Channel channel) {
            Preconditions.checkArgument(channel != null, "channel can not be null");
            this.channel = channel;
        }

        public Builder queueName(String queueName) {
            this.queueName = queueName;
            return this;
        }
        public Builder declareMode(DeclareMode declareMode) {
            this.declareMode = declareMode;
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
        public Builder exclusive(boolean isExclusive) {
            this.isExclusive = isExclusive;
            return this;
        }
        public Builder arguments(Map<String, Object> arguments) {
            this.arguments = arguments;
            return this;
        }

        public RabbitMQQueue build() throws IOException {
            return new RabbitMQQueue(channel, queueName, declareMode, isDurable, isAutoDelete, isExclusive, arguments);
        }
    }
}
