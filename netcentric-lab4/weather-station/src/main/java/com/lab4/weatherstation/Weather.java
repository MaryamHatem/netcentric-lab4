package com.lab4.weatherstation;
public class Weather {

    private int humidity;
    private int temperature;
    private int wind_speed;

    public Weather(int humidity, int temperature, int wind_speed) {
        this.humidity = humidity;

        this.temperature = temperature;

        this.wind_speed = wind_speed;
    }

    public int getHumidity() {
        return humidity;
    }


    public int getTemperature() {
        return temperature;
    }


    public int getWind_speed() {
        return wind_speed;
    }
}
