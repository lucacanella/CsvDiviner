package org.lucacanella.csvdiviner.Core.Log;

public interface DivinerLoggerInterface {
    public void setLevel(LoggerLevel level);
    public LoggerLevel getLevel();
    public void logWarn(String warningTemplate, Object... params);
    public void logInfo(String infoTemplate, Object ...params);
    public void logErrorWithStacktrace(Exception exc, String errorTemplate, String ...params);
    public void logError(Exception exc, String errorTemplate, String ...params);
    public void logCritical(Error exc, String errorTemplate, String ...params);
}
