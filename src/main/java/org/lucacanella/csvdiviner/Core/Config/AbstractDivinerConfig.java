package org.lucacanella.csvdiviner.Core.Config;

import org.lucacanella.csvdiviner.Core.Log.LoggerLevel;

public class AbstractDivinerConfig {

    protected LoggerLevel loggerLevel;

    public void setLoggerLevel(LoggerLevel loggerLevel) {
        this.loggerLevel = loggerLevel;
    }

    public LoggerLevel getLoggerLevel() {
        return loggerLevel;
    }

}
