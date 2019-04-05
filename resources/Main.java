package com.turingml;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        // create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Set Kafka properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "{{ broker_servers }}");
        properties.setProperty("group.id", "{{ group_id }}");

        // dont add any spaces between the brakets and the word
        // Issue: https://github.com/hoisie/mustache/pull/50
        {{{source_streams}}}

        {{{functions}}}

        {{{sink_stream}}}

        env.execute();
    }
}