package com.lab4.centralstation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WeatherReading {
    private Long station_id;
    private Long s_no;
    private String battery_status;
    private Long status_timestamp;
    private Weather weather;

    // Getters and setters
    public Long getStation_id() { return station_id; }
    public void setStation_id(Long station_id) { this.station_id = station_id; }
    
    public Long getS_no() { return s_no; }
    public void setS_no(Long s_no) { this.s_no = s_no; }
    
    public String getBattery_status() { return battery_status; }
    public void setBattery_status(String battery_status) { this.battery_status = battery_status; }
    
    public Long getStatus_timestamp() { return status_timestamp; }
    public void setStatus_timestamp(Long status_timestamp) { this.status_timestamp = status_timestamp; }
    
    public Weather getWeather() { return weather; }
    public void setWeather(Weather weather) { this.weather = weather; }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Weather {
        private Integer humidity;
        private Integer temperature;
        private Integer wind_speed;

        public Integer getHumidity() { return humidity; }
        public void setHumidity(Integer humidity) { this.humidity = humidity; }
        
        public Integer getTemperature() { return temperature; }
        public void setTemperature(Integer temperature) { this.temperature = temperature; }
        
        public Integer getWind_speed() { return wind_speed; }
        public void setWind_speed(Integer wind_speed) { this.wind_speed = wind_speed; }
    }
}