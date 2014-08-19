package org.apache.flume.rabbitmq;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;

public class RabbitMqConnectionFactoryTest {
    @Test
    public void testCanCreateConnectionFactoryWithNoExplicitParameters() {
        Assert.assertNotNull(new RabbitMQConnectionFactory.Builder().build());
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwIAEWhenHostIsNull() {
        Assert.assertNotNull(new RabbitMQConnectionFactory.Builder()
                .addHost(null).build());
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwIAEWhenHostsIsNull() {
        Assert.assertNotNull(new RabbitMQConnectionFactory.Builder()
                .addHosts(null).build());
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwIAEWhenUserNameIsNull() {
        Assert.assertNotNull(new RabbitMQConnectionFactory.Builder()
                .setUserName(null).build());
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwIAEWhenPasswordIsNull() {
        Assert.assertNotNull(new RabbitMQConnectionFactory.Builder()
                .setPassword(null).build());
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwIAEWhenVHostIsNull() {
        Assert.assertNotNull(new RabbitMQConnectionFactory.Builder()
                .setVirtualHost(null).build());
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwIAEWhenConnectionFactoryIsNull() {
        RabbitMQConnectionFactory.Builder.setConnectionFactory(null);
        Assert.assertNotNull(new RabbitMQConnectionFactory.Builder().build());
    }

    @Test
    public void testCreateSingleHostConnectionWithPort() throws IOException {
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        RabbitMQConnectionFactory.Builder.setConnectionFactory(connectionFactory);
        new RabbitMQConnectionFactory.Builder()
                .setUserName("user").setPort(123).build().createConnection();
        verify(connectionFactory).setUsername("user");
        verify(connectionFactory).setPort(123);
        verify(connectionFactory).newConnection();
    }

    @Test
    public void testCreateSingleHostConnectionWithHostNotContaingPort()
            throws IOException {
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        RabbitMQConnectionFactory.Builder.setConnectionFactory(connectionFactory);
        new RabbitMQConnectionFactory.Builder()
                .addHost("host").build().createConnection();
        verify(connectionFactory).setHost("host");
        verify(connectionFactory).setPort(ConnectionFactory.DEFAULT_AMQP_PORT);
        verify(connectionFactory).newConnection();
    }

    @Test
    public void testCreateSingleHostConnectionWithHostContaingPort()
            throws IOException {
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        RabbitMQConnectionFactory.Builder.setConnectionFactory(connectionFactory);
        new RabbitMQConnectionFactory.Builder()
                .addHost("host:9999").build().createConnection();
        verify(connectionFactory).setHost("host");
        verify(connectionFactory).setPort(ConnectionFactory.DEFAULT_AMQP_PORT);
        verify(connectionFactory).setPort(9999);
        verify(connectionFactory).newConnection();
    }

    @Test
    public void testCreateMultipleHostConnection() throws IOException {
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        RabbitMQConnectionFactory.Builder.setConnectionFactory(connectionFactory);
        new RabbitMQConnectionFactory.Builder()
                .addHost("host1:9999").addHost("host2").build().createConnection();
        verify(connectionFactory, never()).setHost("host1");
        verify(connectionFactory, never()).setHost("host2");
        verify(connectionFactory, never()).setPort(9999);
        verify(connectionFactory).setPort(ConnectionFactory.DEFAULT_AMQP_PORT);
        verify(connectionFactory).newConnection(new Address[]
                {Address.parseAddress("host1:9999"), Address.parseAddress("host2")});
    }
}
