package com.lab4.centralstation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class AivenConfig {
    // Get these from your Aiven service
    private static final String HOST = "pg-60a663d-xxx.aivencloud.com"; // Replace with your actual host
    private static final String PORT = "12345"; // Replace with your actual port
    private static final String DATABASE = "weatherdb";
    private static final String USER = "avnadmin";
    private static final String PASSWORD = "YOUR_ACTUAL_PASSWORD"; // Replace with your actual password
    
    public static Connection getConnection() throws SQLException {
        String jdbcUrl = String.format(
            "jdbc:postgresql://%s:%s/%s?sslmode=require",
            HOST, PORT, DATABASE
        );
        return DriverManager.getConnection(jdbcUrl, USER, PASSWORD);
    }
}