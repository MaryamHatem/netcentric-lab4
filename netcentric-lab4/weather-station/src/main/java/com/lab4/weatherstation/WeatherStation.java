package com.lab4.weatherstation;//by3ml weather msgs

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Random;
import java.util.Properties;

public class WeatherStation {

    private static final long STATION_ID = resolveStationId();

// Reads STATION_ID from env var if set, otherwise falls back to the
// pod ordinal from the hostname (StatefulSet gives weather-station-0, -1, ...).
private static long resolveStationId() {
    String envId = System.getenv("STATION_ID");
    if (envId != null && !envId.isBlank()) {
        return Long.parseLong(envId.trim());
    }
    String hostname = System.getenv().getOrDefault("HOSTNAME", "");
    int dash = hostname.lastIndexOf('-');
    if (dash != -1) {
        try {
            return Integer.parseInt(hostname.substring(dash + 1)) + 1;
        } catch (NumberFormatException ignored) {
            // fall through to default
        }
    }
    return 1;
}
    private static final Random random = new Random();
    private static final ObjectMapper objectMapper = new ObjectMapper();//by3ml convertor

    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();

        properties.setProperty(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "kafka:9092"
        );

        properties.setProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        properties.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        long sequenceNumber = 1;

        while (true) {

            // Randomly drop 10% of messages
            if (random.nextDouble() >= 0.10) {

                String batteryStatus = generateBatteryStatus();
                int humidity = 20 + random.nextInt(71);//bein 20 w 90%
                int temperature = 10 + random.nextInt(31);//bein 10 w 40
                int windSpeed = random.nextInt(21);//men 0 l 20
                long status_timestamp = System.currentTimeMillis() / 1000;//akhaleeeha sec
                Weather weather = new Weather(
                        humidity,
                        temperature,
                        windSpeed
                );

                WeatherReading reading = new WeatherReading(
                    STATION_ID,
                    sequenceNumber,
                    batteryStatus,
                    status_timestamp,
                    weather
            );

                String json = objectMapper.writeValueAsString(reading);

                System.out.println(json);

                ProducerRecord<String, String> record =
                        new ProducerRecord<>("weather-readings", json);

                producer.send(record);
            }

            sequenceNumber++;

            Thread.sleep(1000);//astana sania
        }
    }

    private static String generateBatteryStatus() {

        double value = random.nextDouble();

        if (value < 0.30) {
            return "low";
        } else if (value < 0.70) {
            return "medium";
        } else {
            return "high";
        }
    }
}
