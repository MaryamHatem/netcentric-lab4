package com.lab4.weatherstation; 


public class WeatherReading { // This class byrepresents  weather status wa7da

    private long station_id;//elid
    private long s_no;//sequence number
    private String battery_status;//status low, medium, high
    private long status_timestamp;// Stores the time elli status was generated.
    private Weather weather;

     // by3ml WeatherReading
    public WeatherReading(long station_id,long s_no,
                      String battery_status,long status_timestamp,Weather weather) {

        this.station_id = station_id;
        this.s_no = s_no;
        this.battery_status = battery_status;
        this.status_timestamp = status_timestamp;
        this.weather = weather;
    }


    public long getStation_id() {
   
        return station_id;
    }


    public long getS_no() {

        return s_no;
    }


    public String getBattery_status() {

        return battery_status;
    }


    public long getStatus_timestamp() {

        return status_timestamp;
    }


    public Weather getWeather() {
        return weather;
    }
}