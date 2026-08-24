package com.lab4.centralstation;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class CentralStation {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final int BATCH_SIZE = 5000;
    private static final int FLUSH_INTERVAL_MS = 5000; // 5 seconds

    // Batch buffers
    private static final List<WeatherReading> readingBuffer = new ArrayList<>();

    // Database connection
    private static Connection dbConnection;

    // JDBC batch prepared statements
    private static PreparedStatement insertStmt;

    // Last flush time
    private static long lastFlushTime = System.currentTimeMillis();

    // Counter for total inserted
    private static final AtomicLong totalInserted = new AtomicLong(0);

    public static void main(String[] args) {
        try {
            // ========== MODIFIED: Aiven PostgreSQL Configuration ==========
            // Get configuration from environment variables
            String bootstrapServers = getEnv("KAFKA_BOOTSTRAP", "localhost:9092");
            
            // Aiven PostgreSQL connection details
            String dbHost = getEnv("DB_HOST", "pg-60a663d-weather-monitoring.i.aivencloud.com"); 
            String dbPort = getEnv("DB_PORT", "25022"); 
            String dbName = getEnv("DB_NAME", "weatherdb");
            String dbUser = getEnv("DB_USER", "avnadmin");
            String dbPassword = getEnv("DB_PASSWORD", "AVNS_iUR15hUXWziHYQOxsWT"); 
            
            // Build PostgreSQL JDBC URL with SSL
            String dbUrl = String.format(
                "jdbc:postgresql://%s:%s/%s?sslmode=require",
                dbHost, dbPort, dbName
            );

            System.out.println("========================================");
            System.out.println("Central Station started!");
            System.out.println("Kafka: " + bootstrapServers);
            System.out.println("Database: " + dbUrl);
            System.out.println("Batch size: " + BATCH_SIZE);
            System.out.println("Flush interval: " + FLUSH_INTERVAL_MS + "ms");
            System.out.println("========================================");

            // 2. Connect to database (Aiven PostgreSQL)
            connectToDatabase(dbUrl, dbUser, dbPassword);

            // 3. Create Kafka consumer
            KafkaConsumer<String, String> consumer = createKafkaConsumer(bootstrapServers);
            consumer.subscribe(Collections.singletonList("weather-readings"));

            System.out.println("Subscribed to topic: weather-readings");
            System.out.println("Waiting for messages...");
            System.out.println("========================================");

            // 4. Main consumption loop
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // Parse the JSON
                        WeatherReading reading = mapper.readValue(record.value(), WeatherReading.class);

                        // Print received message
                        System.out.println("Received: Station " + reading.getStation_id() +
                                " | Seq " + reading.getS_no() +
                                " | Temp " + reading.getWeather().getTemperature() + "°F" +
                                " | Battery: " + reading.getBattery_status());

                        // Add to buffer
                        synchronized (readingBuffer) {
                            readingBuffer.add(reading);
                        }

                        // Check if buffer reached batch size
                        if (readingBuffer.size() >= BATCH_SIZE) {
                            flushReadings();
                        }

                        // Check if flush interval elapsed
                        if (System.currentTimeMillis() - lastFlushTime >= FLUSH_INTERVAL_MS && !readingBuffer.isEmpty()) {
                            flushReadings();
                        }

                    } catch (Exception e) {
                        System.err.println("Error processing message: " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            }

        } catch (Exception e) {
            System.err.println("Fatal error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void connectToDatabase(String url, String user, String password) throws SQLException {
        // Load PostgreSQL driver
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.err.println("PostgreSQL JDBC Driver not found!");
            e.printStackTrace();
            throw new SQLException("Driver not found");
        }
        
        dbConnection = DriverManager.getConnection(url, user, password);
        dbConnection.setAutoCommit(false);

        // Prepare the insert statement (PostgreSQL syntax - uses BIGSERIAL auto-increment)
        String insertSql = "INSERT INTO weather_readings " +
                "(station_id, sequence_number, battery_status, timestamp, humidity, temperature, wind_speed) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)";
        insertStmt = dbConnection.prepareStatement(insertSql);

        System.out.println("✅ Connected to Aiven PostgreSQL successfully!");
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "central-station-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return new KafkaConsumer<>(props);
    }

    private static void flushReadings() {
        synchronized (readingBuffer) {
            if (readingBuffer.isEmpty()) {
                return;
            }

            try {
                int count = 0;
                for (WeatherReading reading : readingBuffer) {
                    insertStmt.setLong(1, reading.getStation_id());
                    insertStmt.setLong(2, reading.getS_no());
                    insertStmt.setString(3, reading.getBattery_status());
                    insertStmt.setLong(4, reading.getStatus_timestamp());
                    insertStmt.setInt(5, reading.getWeather().getHumidity());
                    insertStmt.setInt(6, reading.getWeather().getTemperature());
                    insertStmt.setInt(7, reading.getWeather().getWind_speed());
                    insertStmt.addBatch();
                    count++;
                }

                // Execute batch
                int[] results = insertStmt.executeBatch();
                dbConnection.commit();

                // Clear buffer and update counters
                readingBuffer.clear();
                totalInserted.addAndGet(count);
                lastFlushTime = System.currentTimeMillis();

                System.out.println("----------------------------------------");
                System.out.println("✅ Flushed " + count + " readings to Aiven PostgreSQL (total: " + totalInserted.get() + ")");
                System.out.println("----------------------------------------");

            } catch (SQLException e) {
                System.err.println("Error flushing readings: " + e.getMessage());
                try {
                    dbConnection.rollback();
                } catch (SQLException rollbackEx) {
                    System.err.println("Rollback failed: " + rollbackEx.getMessage());
                }
                e.printStackTrace();
            }
        }
    }

    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null ? value : defaultValue;
    }
}