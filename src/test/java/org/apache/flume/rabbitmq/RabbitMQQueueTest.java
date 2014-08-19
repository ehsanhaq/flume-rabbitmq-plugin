package org.apache.flume.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.impl.AMQImpl;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.apache.flume.rabbitmq.RabbitMQQueue.DeclareMode;
import static org.mockito.Mockito.*;

public class RabbitMQQueueTest {
    @Test (expected = IllegalArgumentException.class)
    public void throwIAEWhenPassingNullToBuilderConstructor() {
        new RabbitMQQueue.Builder(null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void throwIAEWhenPassiveAndNoQueueNameIsProvided() throws IOException {
        Channel channel = mock(Channel.class);
        new RabbitMQQueue.Builder(channel).declareMode(DeclareMode.PASSIVE).build();
    }

    @Test (expected = IOException.class)
    public void throwIOExceptionWhileDeclarePassiveQueue() throws IOException {
        Channel channel = mock(Channel.class);
        when(channel.queueDeclarePassive("test-queue")).thenThrow(
                new IOException("IOException while creating queue"));
        new RabbitMQQueue.Builder(channel).declareMode(DeclareMode.PASSIVE).queueName("test-queue").build();
    }
    @Test (expected = IOException.class)
    public void throwIOExceptionWhileDeclareActiveDefaultServerQueue() throws IOException {
        Channel channel = mock(Channel.class);
        when(channel.queueDeclare()).thenThrow(new IOException("IOException while creating queue"));
        new RabbitMQQueue.Builder(channel).declareMode(DeclareMode.ACTIVE).build();
    }
    @Test (expected = IOException.class)
    public void throwIOExceptionWhileDeclareActiveQueue() throws IOException {
        Channel channel = mock(Channel.class);
        when(channel.queueDeclare("test-queue", false, false, false, null))
                .thenThrow(new IOException("IOException while creating queue"));
        new RabbitMQQueue.Builder(channel).queueName("test-queue").declareMode(DeclareMode.ACTIVE)
                .autoDelete(false).durable(false).exclusive(false).arguments(null)
                .build();
    }

    @Test
    public void testDeclarePassiveQueue() throws IOException {
        Channel channel = mock(Channel.class);
        Assert.assertNotNull(new RabbitMQQueue.Builder(channel).queueName("test-queue")
                .declareMode(DeclareMode.PASSIVE)
                .build());
        verify(channel).queueDeclarePassive("test-queue");
        verifyNoMoreInteractions(channel);
    }

    @Test
    public void testDeclareActiveServerQueue() throws IOException {
        Channel channel = mock(Channel.class);
        when(channel.queueDeclare()).thenReturn(new AMQImpl.Queue.DeclareOk("serverQueueName", 0, 0));
        Assert.assertNotNull(new RabbitMQQueue.Builder(channel).declareMode(DeclareMode.ACTIVE).build());
        verify(channel).queueDeclare();
        verifyNoMoreInteractions(channel);
    }

    @Test
    public void testDeclareActiveQueue() throws IOException {
        Channel channel = mock(Channel.class);
        Assert.assertNotNull(new RabbitMQQueue.Builder(channel).queueName("test-queue")
                .declareMode(DeclareMode.ACTIVE)
                .autoDelete(true).durable(true).exclusive(true)
                .arguments(null)
                .build());
        verify(channel).queueDeclare("test-queue", true, true, true, null);
        verifyNoMoreInteractions(channel);
    }
}
