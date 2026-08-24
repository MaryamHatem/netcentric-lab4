package com.lab4.weatherstation;

import java.sql.*;
import java.util.List;

public class WeatherDatabase {
    private static final int BATCH_SIZE = 5000;
    
    // Aiven PostgreSQL connection
    private static final String DB_HOST = "pg-60a663d-YOUR_ACTUAL_HOST.aivencloud.com";  // ← REPLACE
    private static final String DB_PORT = "YOUR_ACTUAL_PORT";                           // ← REPLACE
    private static final String DB_NAME = "weatherdb";
    private static final String DB_USER = "avnadmin";
    private static final String DB_PASSWORD = "YOUR_ACTUAL_PASSWORD";                   // ← REPLACE
    
    public void insertBatch(List<WeatherReading> readings) {
        if (readings.isEmpty()) return;
        
        String jdbcUrl = String.format(
            "jdbc:postgresql://%s:%s/%s?sslmode=require",
            DB_HOST, DB_PORT, DB_NAME
        );
        
        String sql = "INSERT INTO weather_readings " +
                    "(station_id, sequence_number, battery_status, timestamp, " +
                    "humidity, temperature, wind_speed) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        try {
            // Load PostgreSQL driver
            Class.forName("org.postgresql.Driver");
            
            try (Connection conn = DriverManager.getConnection(jdbcUrl, DB_USER, DB_PASSWORD)) {
                conn.setAutoCommit(false);
                
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    int count = 0;
                    
                    for (WeatherReading reading : readings) {
                        // CORRECT method names - match your WeatherReading class
                        pstmt.setLong(1, reading.getStation_id());
                        pstmt.setLong(2, reading.getS_no());           // ← FIXED: was getSequenceNumber()
                        pstmt.setString(3, reading.getBattery_status()); // ← FIXED: was getBatteryStatus()
                        pstmt.setLong(4, reading.getStatus_timestamp()); // ← FIXED: was getStringTimestamp()
                        pstmt.setInt(5, reading.getWeather().getHumidity());
                        pstmt.setInt(6, reading.getWeather().getTemperature());
                        pstmt.setInt(7, reading.getWeather().getWind_speed());
                        pstmt.addBatch();
                        count++;
                        
                        if (count % BATCH_SIZE == 0) {
                            pstmt.executeBatch();
                            conn.commit();
                            System.out.println("✅ Flushed " + BATCH_SIZE + " records to Aiven PostgreSQL");
                        }
                    }
                    
                    // Flush remaining records
                    pstmt.executeBatch();
                    conn.commit();
                    System.out.println("✅ Flushed " + count + " total records to Aiven PostgreSQL");
                }
            }
        } catch (ClassNotFoundException e) {
            System.err.println("PostgreSQL JDBC Driver not found!");
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Error inserting batch: " + e.getMessage());
            e.printStackTrace();
        }
    }
}