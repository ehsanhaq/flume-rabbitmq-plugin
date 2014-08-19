# Flume RabbitMQ Plugin
The **Flume Rabbit MQ plugin** project provides ``Flume`` plugin for using ``RabbitMQ`` as a source in flume.

## Running the unit tests
To run the unit tests

`mvn test`

## Integration tests

### Setting up the environment for Integration tests
* Install RabbitMQ server.
* Run the RabbitMQ server.
* Verify that the server is listening on port 5672.
* Verify that the default user and password for RabbitMQ is guest/guest.

### Running Integration tests
`mvn integration-test`

## Deploying
* Log on to the machine where you want to deploy the RabbitMQ Flume plugin.
* Create a `plugins.d` directory in the `$FLUME_HOME`.
* Unzip the zip artifact of the project in `$FLUME_HOME/plugins.d` directory.
* Start the flume agent. 