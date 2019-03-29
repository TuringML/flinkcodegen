package com.turingml;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 * Simple example on how to read with a Kafka consumer
 *
 * Note that the Kafka source is expecting the following parameters to be set -
 * "bootstrap.servers" (comma separated list of kafka brokers) -
 * "zookeeper.connect" (comma separated list of zookeeper servers) -
 * "topic.from" the name of the topic to read data from.
 *
 * You can pass these required parameters using "--bootstrap.servers
 * host:port,host1:port1 --zookeeper.connect host:port --topic testTopic"
 *
 * This is a valid input example: --topic.from test --bootstrap.servers
 * localhost:9092 --zookeeper.connect localhost:2181
 */
public class Main {
    public static void main(String[] args) {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // parse user parameters
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        {{{streams}}}

        {{{functions}}}

        env.execute();
    }
}