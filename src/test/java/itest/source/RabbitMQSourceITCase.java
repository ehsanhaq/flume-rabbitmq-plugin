package itest.source;

import com.google.common.collect.Lists;
import itest.setup.RabbitMQManagement;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.rabbitmq.RabbitMQConfigurationConstants;
import org.apache.flume.rabbitmq.source.RabbitMQSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.List;

import static org.apache.flume.PollableSource.Status;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class RabbitMQSourceITCase {
    RabbitMQManagement rabbitMQManagement;

    static MemoryChannel channel;
    static RabbitMQSource source;
    ChannelProcessor channelProcessor;
    List<Event> events;

    private String exchangeName = "foo-exchange";
    private String queueName = "foo-queue";

    @Before
    public void setUp() throws IOException {
        rabbitMQManagement = new RabbitMQManagement();
        rabbitMQManagement.setup();
        rabbitMQManagement.createExchange(exchangeName, "fanout");
        rabbitMQManagement.createQueue(queueName);
        rabbitMQManagement.bindQueue(queueName, exchangeName, "");

        source = new RabbitMQSource();
        channel = new MemoryChannel();

        Configurables.configure(channel, new Context());

        channelProcessor = mock(ChannelProcessor.class);
        events = Lists.newArrayList();
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                events.addAll((List<Event>)invocation.getArguments()[0]);
                return null;
            }
        }).when(channelProcessor).processEventBatch(any(List.class));
        source.setChannelProcessor(channelProcessor);
    }

    @After
    public void tearDown() throws IOException {
        rabbitMQManagement.deleteExchange(exchangeName);
        rabbitMQManagement.deleteQueue(queueName);
        rabbitMQManagement.tearDown();
    }

    @Test
    public void testLifeCycle() throws InterruptedException {
        Context context = new Context();
        context.put(RabbitMQConfigurationConstants.EXCHANGE_NAME, exchangeName);
        Configurables.configure(source, context);
        for (int i=0; i<10; i++) {
            source.start();
            Assert.assertTrue("Reached start or error", LifecycleController.waitForOneOf(source,
                    LifecycleState.START_OR_ERROR));
            Assert.assertEquals("Server is started", LifecycleState.START,source.getLifecycleState());

            source.stop();
            Assert.assertTrue("Reached stop or error", LifecycleController.waitForOneOf(source,
                    LifecycleState.STOP_OR_ERROR));
            Assert.assertEquals("Server is stopped", LifecycleState.STOP, source.getLifecycleState());
        }
    }

    @Test
    public void testWithServerQueueNoBinding() throws EventDeliveryException, IOException {
        Context context = new Context();
        context.put(RabbitMQConfigurationConstants.EXCHANGE_NAME, exchangeName);
        Configurables.configure(source, context);
        source.start();

        Status status = source.process();
        Assert.assertEquals(Status.BACKOFF, status);
        Assert.assertEquals(0, events.size());
        rabbitMQManagement.publish(exchangeName, "", "This is a test message");
        status = source.process();
        Assert.assertEquals(Status.BACKOFF, status);
        Assert.assertEquals(0, events.size());
        source.stop();
    }

    @Test
    public void testWithServerQueueTopicBinding() throws EventDeliveryException, IOException {
        Context context = new Context();
        context.put(RabbitMQConfigurationConstants.EXCHANGE_NAME, exchangeName);
        context.put(RabbitMQConfigurationConstants.TOPICS, "*");
        Configurables.configure(source, context);
        source.start();

        Status status = source.process();
        Assert.assertEquals(Status.BACKOFF, status);
        Assert.assertEquals(0, events.size());
        rabbitMQManagement.publish(exchangeName, "", "This is a test message");
        Assert.assertEquals(Status.READY, source.process());
        Assert.assertEquals(1, events.size());
        Assert.assertArrayEquals("This is a test message".getBytes(), events.get(0).getBody());
        source.stop();
    }

    @Test
    public void testWithExplicitQueue() throws EventDeliveryException, IOException {
        Context context = new Context();
        context.put(RabbitMQConfigurationConstants.EXCHANGE_NAME, exchangeName);
        context.put(RabbitMQConfigurationConstants.QUEUE_NAME, queueName);
        Configurables.configure(source, context);
        source.start();

        Assert.assertEquals(Status.BACKOFF, source.process());
        Assert.assertEquals(0, events.size());
        rabbitMQManagement.publish(exchangeName, "", "This is a test message");
        Assert.assertEquals(Status.READY, source.process());
        Assert.assertEquals(1, events.size());
        Assert.assertArrayEquals("This is a test message".getBytes(), events.get(0).getBody());
        source.stop();
    }

    @Test
    public void testMultipleMessages() throws EventDeliveryException, IOException {
        Context context = new Context();
        context.put(RabbitMQConfigurationConstants.EXCHANGE_NAME, exchangeName);
        context.put(RabbitMQConfigurationConstants.QUEUE_NAME, queueName);
        Configurables.configure(source, context);
        source.start();
        for (int i=0; i<10; i++) {
            rabbitMQManagement.publish(exchangeName, "", "Message " + i);
        }
        Assert.assertEquals(Status.READY, source.process());
        Assert.assertEquals(10, events.size());
        for (int i=0; i<10; i++) {
            Assert.assertArrayEquals(("Message " + i).getBytes(), events.get(i).getBody());
        }
        source.stop();
    }

    @Test (expected = EventDeliveryException.class)
    public void testQueueDeletedWhileProcessing() throws IOException, EventDeliveryException {
        Context context = new Context();
        context.put(RabbitMQConfigurationConstants.EXCHANGE_NAME, exchangeName);
        context.put(RabbitMQConfigurationConstants.QUEUE_NAME, queueName);
        context.put(RabbitMQConfigurationConstants.BATCH_SIZE, "5");
        Configurables.configure(source, context);
        source.start();
        for (int i=0; i<10; i++) {
            rabbitMQManagement.publish(exchangeName, "", "Message " + i);
        }
        Assert.assertEquals(Status.READY, source.process());
        Assert.assertEquals(5, events.size());
        rabbitMQManagement.deleteQueue(queueName);
        source.process();
    }

    @Test
    public void testCreateExplicitQueue() throws IOException, EventDeliveryException {
        String queueName = "bar-queue";
        Context context = new Context();
        context.put(RabbitMQConfigurationConstants.EXCHANGE_NAME, exchangeName);
        context.put(RabbitMQConfigurationConstants.QUEUE_NAME, queueName);
        context.put(RabbitMQConfigurationConstants.TOPICS, "*");
        Configurables.configure(source, context);
        source.start();
        for (int i=0; i<10; i++) {
            rabbitMQManagement.publish(exchangeName, "", "Message " + i);
        }
        Assert.assertEquals(Status.BACKOFF, source.process());
        rabbitMQManagement.deleteQueue("bar-queue");
    }
    //TODO: Add case for Exchange deleted while processing
}