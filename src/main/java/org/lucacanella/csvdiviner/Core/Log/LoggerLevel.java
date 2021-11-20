package org.lucacanella.csvdiviner.Core.Log;

public enum LoggerLevel {
    OFF(4),
    INFO(3),
    WARNING(2),
    ERROR(1);

    private final int value;

    LoggerLevel(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
