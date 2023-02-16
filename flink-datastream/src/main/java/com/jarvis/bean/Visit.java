package com.jarvis.bean;

public class Visit {
    private long timestamp;
    private String domain;
    private double traffic;

    public Visit() {
    }

    public Visit(long timestamp, String domain, double traffic) {
        this.timestamp = timestamp;
        this.domain = domain;
        this.traffic = traffic;
    }

    @Override
    public String toString() {
        return "Visit{" +
                "timestamp=" + timestamp +
                ", domain='" + domain + '\'' +
                ", traffic=" + traffic +
                '}';
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public double getTraffic() {
        return traffic;
    }

    public void setTraffic(double traffic) {
        this.traffic = traffic;
    }
}
