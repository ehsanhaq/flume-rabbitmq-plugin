package org.apache.flume.rabbitmq;

import com.google.common.collect.Maps;
import com.rabbitmq.client.Channel;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.mockito.Mockito.*;

public class RabbitMQExchangeTest {
    @Test (expected = IllegalArgumentException.class)
    public void throwIAEWhenNullIsPassedToBuilderConstructorArgs() {
        new RabbitMQExchange.Builder(null, null);
    }
    @Test (expected = IllegalArgumentException.class)
    public void throwIAEWhenBuilderIsConstructedWithNullChannel() {
        new RabbitMQExchange.Builder(null, "exchange-name");
    }
    @Test (expected = IllegalArgumentException.class)
    public void throwIAEWhenBuilderIsConstructedWithNullExchangeName() {
        new RabbitMQExchange.Builder(mock(Channel.class), null);
    }

    @Test (expected = IOException.class)
    public void throwIOExceptionWhilePassiveExchangeDeclare() throws IOException {
        Channel channel = mock(Channel.class);
        when(channel.exchangeDeclarePassive("test-exchange")).thenThrow(new IOException("IOException while creating exchange"));
        new RabbitMQExchange.Builder(channel, "test-exchange").build();
    }

    @Test (expected = IOException.class)
    public void throwIOExceptionWhileActiveExchangeDeclare() throws IOException {
        Channel channel = mock(Channel.class);
        when(channel.exchangeDeclare("test-exchange", "abc", false, false, false, null))
                .thenThrow(new IOException("IOException while creating exchange"));
        new RabbitMQExchange.Builder(channel, "test-exchange").exchangeType("abc")
                .passive(false).autoDelete(false).durable(false).internal(false).arguments(null)
                .build();
    }

    @Test
    public void testExchangePassiveDeclare() throws IOException {
        Channel channel = mock(Channel.class);
        Assert.assertNotNull(new RabbitMQExchange.Builder(channel, "test-exchange")
                .passive(true)   // It is passive by default as well
                .autoDelete(true).durable(true).exchangeType("abc")  // These will be ignored since it is
                // passive.
                .build());
        verify(channel).exchangeDeclarePassive("test-exchange");
        verifyNoMoreInteractions(channel);
    }

    @Test
    public void testActiveExchangeDeclare() throws IOException {
        Channel channel = mock(Channel.class);
        Assert.assertNotNull(new RabbitMQExchange.Builder(channel, "test-exchange")
                .passive(false)
                .autoDelete(true).durable(true).exchangeType("abc")
                .build());
        Map<String, Object> properties = Maps.newHashMap();
        verify(channel).exchangeDeclare("test-exchange", "abc", true, true, false, properties);
        verifyNoMoreInteractions(channel);
    }

    @Test
    public void testActiveExchangeDeclareWithProperties() throws IOException {
        Channel channel = mock(Channel.class);
        Map<String, Object> properties = Maps.newHashMap();
        properties.put("key", "value");
        Assert.assertNotNull(new RabbitMQExchange.Builder(channel, "test-exchange")
                .passive(false)
                .autoDelete(true).durable(true).exchangeType("fanout").internal(true)
                .arguments(properties)
                .build());
        verify(channel).exchangeDeclare("test-exchange", "fanout", true, true, true, properties);
        verifyNoMoreInteractions(channel);
    }
}
