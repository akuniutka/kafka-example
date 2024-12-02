package io.github.akuniutka.kafka;

public class WeatherEvent {

    private double latitude;
    private double longitude;
    private double temperature;

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "WeatherEvent{" +
                "latitude=" + latitude +
                ", longitude=" + longitude +
                ", temperature=" + temperature +
                '}';
    }
}
