package org.apache.flume.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;

public class RabbitMQChannelTest {

    @Test(expected = IllegalArgumentException.class)
    public void throwIAEWhenNullIsPassedToBuilderConstructor() throws IOException {
        new RabbitMQChannel.Builder(null);
    }

    @Test (expected = IOException.class)
    public void throwIOExceptionWhileCreateChannel() throws IOException {
        Connection connection = mock(Connection.class);
        when(connection.createChannel()).thenThrow(new IOException());
        new RabbitMQChannel.Builder(connection).build();
    }

    @Test (expected = IOException.class)
    public void throwIOExceptionWhileCreateChannelWithChannelNumber() throws IOException {
        Connection connection = mock(Connection.class);
        when(connection.createChannel(1234)).thenThrow(new IOException());
        new RabbitMQChannel.Builder(connection).setChannelNumber(1234).build();
    }

    @Test
    public void testPositiveDefaultWithMocks() throws IOException {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(connection.createChannel()).thenReturn(channel);
        Assert.assertEquals(channel,
                new RabbitMQChannel.Builder(connection).build().getChannel());
        verify(connection).createChannel();
        verifyNoMoreInteractions(connection);
    }

    @Test
    public void testPositiveWithProvidedChannelNumberWithMocks() throws IOException {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(connection.createChannel(1234)).thenReturn(channel);
        Assert.assertEquals(channel,
                new RabbitMQChannel.Builder(connection).setChannelNumber(1234).build().getChannel());
        verify(connection).createChannel(1234);
        verifyNoMoreInteractions(connection);
    }

    @Test
    public void testAbortChannel() throws IOException {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(connection.createChannel()).thenReturn(channel);
        RabbitMQChannel rmqChannel = new RabbitMQChannel.Builder(connection).build();
        Assert.assertEquals(channel, rmqChannel.getChannel());
        rmqChannel.abortChannel();
        verify(connection).createChannel();
        verifyNoMoreInteractions(connection);
        verify(channel).abort();
        verifyNoMoreInteractions(channel);
    }

    @Test (expected = IllegalArgumentException.class)
    public void throwIAEWhenBindQueueWithExchangeIsCalledWithNullQueueName() throws IOException {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        RabbitMQChannel rmqChannel = new RabbitMQChannel.Builder(connection).build();
        rmqChannel.bindQueueWithExchange("queue", null, new String[0]);
    }
    @Test (expected = IllegalArgumentException.class)
    public void throwIAEWhenBindQueueWithExchangeIsCalledWithNullExchangeName() throws IOException {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        RabbitMQChannel rmqChannel = new RabbitMQChannel.Builder(connection).build();
        rmqChannel.bindQueueWithExchange(null, "exchange", new String[0]);
    }
    @Test (expected = IllegalArgumentException.class)
    public void throwIAEWhenBindQueueWithExchangeIsCalledWithNullTopics() throws IOException {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        RabbitMQChannel rmqChannel = new RabbitMQChannel.Builder(connection).build();
        rmqChannel.bindQueueWithExchange("queue", "exchange", null);
    }
    @Test
    public void testBindQueueWithExchangeWithNoTopics() throws IOException {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(connection.createChannel()).thenReturn(channel);
        RabbitMQChannel rmqChannel = new RabbitMQChannel.Builder(connection).build();
        rmqChannel.bindQueueWithExchange("queue", "exchange", new String[0]);
        verifyNoMoreInteractions(channel);
    }
    @Test
    public void testBindQueueWithExchangeWithTopics() throws IOException {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(connection.createChannel()).thenReturn(channel);
        RabbitMQChannel rmqChannel = new RabbitMQChannel.Builder(connection).build();
        rmqChannel.bindQueueWithExchange("queue", "exchange", new String[] {"a", "b"});
        verify(channel).queueBind("queue", "exchange", "a");
        verify(channel).queueBind("queue", "exchange", "b");
        verifyNoMoreInteractions(channel);
    }
}
