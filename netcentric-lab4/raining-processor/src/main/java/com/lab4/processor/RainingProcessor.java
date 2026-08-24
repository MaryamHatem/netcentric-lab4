package com.lab4.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class RainingProcessor {

    public static void main(String[] args) {

        Properties properties = new Properties();

        properties.setProperty(
                StreamsConfig.APPLICATION_ID_CONFIG,
                "raining-processor"
        );

        properties.setProperty(
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "kafka:9092"
        );

        properties.setProperty(
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName()
        );

        properties.setProperty(
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass().getName()
        );

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> weatherStream =
                builder.stream("weather-readings");

        ObjectMapper objectMapper = new ObjectMapper();

        KStream<String, String> rainingStream = weatherStream.filter(
                (key, json) -> {
                    try {
                        JsonNode root = objectMapper.readTree(json);

                        int humidity = root
                                .get("weather")
                                .get("humidity")
                                .asInt();

                        return humidity > 70;

                    } catch (Exception e) {
                        return false;
                    }
                }
        );

        rainingStream.to("raining-alerts");

        KafkaStreams streams = new KafkaStreams(
                builder.build(),
                properties
        );

        streams.start();

        Runtime.getRuntime().addShutdownHook(
                new Thread(streams::close)
        );
    }
}