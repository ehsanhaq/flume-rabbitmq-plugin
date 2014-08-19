package org.apache.flume.rabbitmq.source;

import com.google.common.collect.Lists;
import com.rabbitmq.client.*;
import com.rabbitmq.client.impl.AMQImpl;
import org.apache.commons.collections.MapUtils;
import org.apache.flume.*;
import org.apache.flume.Channel;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.rabbitmq.RabbitMQConnectionFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flume.rabbitmq.RabbitMQConfigurationConstants.*;
import static org.mockito.Mockito.*;

public class MockedRabbitMQSourceTest {

    RabbitMQSource source;
    MemoryChannel memoryChannel;
    Context context;

    @Before
    public void setup() {
        source = new RabbitMQSource();
        memoryChannel = new MemoryChannel();
        Configurables.configure(memoryChannel, new Context());

        context = new Context();

        List<Channel> channels = new ArrayList<Channel>();
        channels.add(memoryChannel);

        ChannelSelector rcs = new ReplicatingChannelSelector();
        rcs.setChannels(channels);

        source.setChannelProcessor(new ChannelProcessor(rcs));
    }

    @Test (expected = IllegalArgumentException.class)
    public void throwIAEWhenExchangeNameOrQueueNameIsMissingInContext() {
        Configurables.configure(source, context);
    }

    @Test
    public void testInteractionsWithEmptyServerQueue() throws EventDeliveryException, IOException {
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        Connection connection = mock(Connection.class);
        com.rabbitmq.client.Channel channel = mock(com.rabbitmq.client.Channel.class);
        when(connectionFactory.newConnection()).thenReturn(connection);
        when(connection.createChannel()).thenReturn(channel);
        when(channel.queueDeclare()).thenReturn(new AMQImpl.Queue.DeclareOk("serverQueue", 0, 0));
        RabbitMQConnectionFactory.Builder.setConnectionFactory(connectionFactory);
        context.put(EXCHANGE_NAME, "test-exchange");
        Configurables.configure(source, context);
        source.start();
        Assert.assertEquals(PollableSource.Status.BACKOFF, source.process());
        source.stop();
        verify(connectionFactory).newConnection();
        verify(connection).createChannel();
        verify(channel).queueDeclare();
        verify(channel).exchangeDeclarePassive("test-exchange");
        verify(channel).basicGet("serverQueue", false);
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(channel);

        Event e = getEvent();
        Assert.assertNull(e);
    }

    @Test
    public void testInteractionsWithSingleMessageServerQueue() throws EventDeliveryException, IOException {
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        Connection connection = mock(Connection.class);
        com.rabbitmq.client.Channel channel = mock(com.rabbitmq.client.Channel.class);
        when(connectionFactory.newConnection()).thenReturn(connection);
        when(connection.createChannel()).thenReturn(channel);
        when(channel.queueDeclare()).thenReturn(new AMQImpl.Queue.DeclareOk("serverQueue", 0, 0));
        RabbitMQConnectionFactory.Builder.setConnectionFactory(connectionFactory);
        context.put(EXCHANGE_NAME, "test-exchange");
        Configurables.configure(source, context);
        source.start();
        Envelope envelope = new Envelope(12345, false, "test-exchange", "");
        AMQP.BasicProperties basicProperties = new AMQP.BasicProperties.Builder()
                .messageId("a145ed7ef").headers(null).build();
        GetResponse getResponse = new GetResponse(envelope, basicProperties, "abc".getBytes(), 0);
        when(channel.basicGet("serverQueue", false)).thenReturn(getResponse);
        Assert.assertEquals(PollableSource.Status.READY, source.process());
        source.stop();
        verify(connectionFactory).newConnection();
        verify(connection).createChannel();
        verify(channel).queueDeclare();
        verify(channel).exchangeDeclarePassive("test-exchange");
        verify(channel).basicGet("serverQueue", false);
        verify(channel).basicAck(12345, false);
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(channel);

        Event e = getEvent();
        Assert.assertEquals("abc", new String(e.getBody()));
        Assert.assertEquals("12345", e.getHeaders().get(HEADER_DELIVERYTAG_KEY));
    }

    @Test
    public void testInteractionsMessageHeader() throws EventDeliveryException, IOException {
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        Connection connection = mock(Connection.class);
        com.rabbitmq.client.Channel channel = mock(com.rabbitmq.client.Channel.class);
        when(connectionFactory.newConnection()).thenReturn(connection);
        when(connection.createChannel()).thenReturn(channel);
        when(channel.queueDeclare()).thenReturn(new AMQImpl.Queue.DeclareOk("serverQueue", 0, 0));
        RabbitMQConnectionFactory.Builder.setConnectionFactory(connectionFactory);
        context.put(EXCHANGE_NAME, "test-exchange");
        Configurables.configure(source, context);
        source.start();
        Envelope envelope = new Envelope(12345, false, "test-exchange", "");
        HashMap<String, Object> header = new HashMap<String, Object>();
        header.put("foo-key", "foo-value");
        header.put("bar-key", "bar-value");
        AMQP.BasicProperties basicProperties = new AMQP.BasicProperties.Builder()
                .messageId("a145ed7ef").headers(header).build();
        GetResponse getResponse = new GetResponse(envelope, basicProperties, "abc".getBytes(), 0);
        when(channel.basicGet("serverQueue", false)).thenReturn(getResponse);
        Assert.assertEquals(PollableSource.Status.READY, source.process());
        source.stop();
        verify(connectionFactory).newConnection();
        verify(connection).createChannel();
        verify(channel).queueDeclare();
        verify(channel).exchangeDeclarePassive("test-exchange");
        verify(channel).basicGet("serverQueue", false);
        verify(channel).basicAck(12345, false);
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(channel);

        Event e = getEvent();
        Assert.assertEquals("abc", new String(e.getBody()));
        Assert.assertEquals("12345", e.getHeaders().get(HEADER_DELIVERYTAG_KEY));
        Map<String, String> actualHeader = e.getHeaders();
        actualHeader.remove("timestamp");
        actualHeader.remove("deliveryTag");
        Assert.assertEquals(header, actualHeader);
    }

    @Test
    public void testInteractionsWithSeveralMessagesServerQueue() throws EventDeliveryException, IOException {
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        Connection connection = mock(Connection.class);
        com.rabbitmq.client.Channel channel = mock(com.rabbitmq.client.Channel.class);
        when(connectionFactory.newConnection()).thenReturn(connection);
        when(connection.createChannel()).thenReturn(channel);
        when(channel.queueDeclare()).thenReturn(new AMQImpl.Queue.DeclareOk("serverQueue", 0, 0));
        RabbitMQConnectionFactory.Builder.setConnectionFactory(connectionFactory);
        context.put(EXCHANGE_NAME, "test-exchange");
        Configurables.configure(source, context);
        source.start();
        final List<GetResponse> responses = Lists.newArrayList();
        for (int i=0; i<10; i++) {
            Envelope envelope = new Envelope(12345+i, false, "test-exchange", "");
            AMQP.BasicProperties basicProperties = new AMQP.BasicProperties.Builder()
                    .messageId("" + i).headers(null).build();
            responses.add(new GetResponse(envelope, basicProperties, ("Message-"+i).getBytes(), 10-i+9));
        }
        responses.add(null);
        doAnswer(createAnswers(responses)).when(channel).basicGet("serverQueue", false);
        // single invocation of source.process() will read max DEFAULT_BATCH_SIZE messages from the queue.
        Assert.assertEquals(PollableSource.Status.READY, source.process());
        verify(connection).createChannel();
        verify(channel).queueDeclare();
        verify(channel).exchangeDeclarePassive("test-exchange");
        verify(channel, times(11)).basicGet("serverQueue", false);
        for (int i=0; i<10; i++) {
            verify(channel).basicAck(12345+i, false);
            Event e = getEvent();
            Assert.assertEquals("Message-"+i, new String(e.getBody()));
            Assert.assertEquals(Integer.toString(12345+i), e.getHeaders().get(HEADER_DELIVERYTAG_KEY));
        }
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(channel);
        source.stop();
    }

    @Test
    public void testInteractionsWithSeveralMessagesServerQueueMultipleInvocationOfProcess()
            throws EventDeliveryException, IOException {
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        Connection connection = mock(Connection.class);
        com.rabbitmq.client.Channel channel = mock(com.rabbitmq.client.Channel.class);
        when(connectionFactory.newConnection()).thenReturn(connection);
        when(connection.createChannel()).thenReturn(channel);
        when(channel.queueDeclare()).thenReturn(new AMQImpl.Queue.DeclareOk("serverQueue", 0, 0));
        RabbitMQConnectionFactory.Builder.setConnectionFactory(connectionFactory);
        context.put(EXCHANGE_NAME, "test-exchange");
        context.put(BATCH_SIZE, "2");
        Configurables.configure(source, context);
        source.start();
        final List<GetResponse> responses = Lists.newArrayList();
        for (int i=0; i<10; i++) {
            Envelope envelope = new Envelope(12345+i, false, "test-exchange", "");
            AMQP.BasicProperties basicProperties = new AMQP.BasicProperties.Builder()
                    .messageId("" + i).headers(null).build();
            responses.add(new GetResponse(envelope, basicProperties, ("Message-"+i).getBytes(), 10-i+9));
        }
        responses.add(null);
        doAnswer(createAnswers(responses)).when(channel).basicGet("serverQueue", false);
        int c=0;
        for (int h=0; h<5; h++) {
            Assert.assertEquals(PollableSource.Status.READY, source.process());
            if (h==0) {
                verify(connection).createChannel();
                verify(channel).queueDeclare();
                verify(channel).exchangeDeclarePassive("test-exchange");
            }
            verify(channel, times(h*2+2)).basicGet("serverQueue", false);
            for (int i = 0; i < 2; i++) {
                Event e = getEvent();
                verify(channel).basicAck(12345 + c, false);
                Assert.assertEquals("Message-" + c, new String(e.getBody()));
                Assert.assertEquals(Integer.toString(12345 + c), e.getHeaders().get(HEADER_DELIVERYTAG_KEY));
                c++;
            }
            Assert.assertNull(getEvent());
        }
        Assert.assertEquals(PollableSource.Status.BACKOFF, source.process());
        verify(channel, times(c+1)).basicGet("serverQueue", false);
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(channel);
        source.stop();
    }

    @Test (expected = EventDeliveryException.class)
    public void throwEventDeliveryExceptionWhenIOExceptionOccursWhileProcess() throws EventDeliveryException, IOException {
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        Connection connection = mock(Connection.class);
        com.rabbitmq.client.Channel channel = mock(com.rabbitmq.client.Channel.class);
        when(connectionFactory.newConnection()).thenReturn(connection);
        when(connection.createChannel()).thenReturn(channel);
        when(channel.queueDeclare()).thenReturn(new AMQImpl.Queue.DeclareOk("serverQueue", 0, 0));
        when(channel.basicGet("serverQueue", false)).thenThrow(new IOException("IO Exception in basicGet"));
        RabbitMQConnectionFactory.Builder.setConnectionFactory(connectionFactory);
        context.put(EXCHANGE_NAME, "test-exchange");
        Configurables.configure(source, context);
        source.start();
        source.process();
    }

    @Test
    public void testWithExplicitQueueUsingActive() throws EventDeliveryException, IOException {
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        Connection connection = mock(Connection.class);
        com.rabbitmq.client.Channel channel = mock(com.rabbitmq.client.Channel.class);
        when(connectionFactory.newConnection()).thenReturn(connection);
        when(connection.createChannel()).thenReturn(channel);
        when(channel.queueDeclare("queue-123", false, false, false, new HashMap<String, Object>()))
                .thenReturn(new AMQImpl.Queue.DeclareOk("queue-123", 0, 0));
        RabbitMQConnectionFactory.Builder.setConnectionFactory(connectionFactory);
        context.put(EXCHANGE_NAME, "test-exchange");
        context.put(QUEUE_NAME, "queue-123");
        context.put(QUEUE_PARAMETERS, "create=true?autodelete=false?durable=false?exclusive=false");
        Configurables.configure(source, context);
        source.start();
        source.process();
        verify(connection).createChannel();
        verify(channel).queueDeclare("queue-123", false, false, false, new HashMap<String, Object>());
        verify(channel).exchangeDeclarePassive("test-exchange");
        verify(channel).basicGet("queue-123", false);
        verifyNoMoreInteractions(connection);
        verifyNoMoreInteractions(channel);
        source.stop();

    }

    private Answer createAnswers(final List<GetResponse> responses) {
        return new Answer() {
            private int count = 0;
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                count++;
                return responses.get(count - 1);
            }
        };
    }

    private Event getEvent() {
        Transaction tx = memoryChannel.getTransaction();
        tx.begin();
        Event e = memoryChannel.take();
        tx.commit();
        tx.close();
        return e;
    }

}
