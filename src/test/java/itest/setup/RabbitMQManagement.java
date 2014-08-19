package itest.setup;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;

public class RabbitMQManagement {
    ConnectionFactory factory;
    Connection connection;
    Channel channel;

    public void setup() throws IOException {
        factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();
    }
    public void createExchange(String exchangeName, String exchangeType) throws IOException {
        channel.exchangeDeclare(exchangeName, exchangeType);
    }
    public void deleteExchange(String exchangeName) throws IOException {
        channel.exchangeDelete(exchangeName);
    }
    public void createQueue(String queueName) throws IOException {
        channel.queueDeclare(queueName, false, false, false, null);
    }
    public void deleteQueue(String queueName) throws IOException {
        channel.queueDelete(queueName);
    }
    public void bindQueue(String queueName, String exchangeName, String topic) throws IOException {
        channel.queueBind(queueName, exchangeName, topic);
    }
    public void publish (String exchangeName, String routingKey, String message) throws IOException {
        channel.basicPublish(exchangeName, routingKey, null, message.getBytes());
    }
    public void tearDown() throws IOException {
        channel.close();
        connection.close();
    }
}
