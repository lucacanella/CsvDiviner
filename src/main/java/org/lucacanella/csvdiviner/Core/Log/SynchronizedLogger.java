package org.lucacanella.csvdiviner.Core.Log;

import java.util.logging.Logger;

public class SynchronizedLogger
        implements DivinerLoggerInterface {

    private LoggerLevel level;

    private static final String EOL = System.lineSeparator();

    public SynchronizedLogger(LoggerLevel level) {
        this.level = level;
    }

    public void setLevel(LoggerLevel level) {
        this.level = level;
    }

    public LoggerLevel getLevel() {
        return level;
    }

    synchronized public void logWarn(String warningTemplate, Object ...params) {
        if(level.getValue() < LoggerLevel.WARNING.getValue()) {
            return;
        }
        System.out.print("[Warning] ");
        System.out.format(warningTemplate, params);
        System.out.print(EOL);
    }

    synchronized public void logInfo(String infoTemplate, Object ...params) {
        if(level.getValue() < LoggerLevel.INFO.getValue()) {
            return;
        }
        System.out.print("[Info] ");
        System.out.format(infoTemplate, params);
        System.out.print(EOL);
    }
    synchronized public void logErrorWithStacktrace(Exception exc, String errorTemplate, String ...params) {
        logError(exc, errorTemplate, params);
        exc.printStackTrace(System.out);
    }

    synchronized public void logError(Exception exc, String errorTemplate, String ...params) {
        if(level.getValue() < LoggerLevel.ERROR.getValue()) {
            return;
        }
        System.out.print("[Error] ");
        System.out.format(errorTemplate, params);
        System.out.format("%s\tTipo eccezione: %s%s", EOL, exc.getClass().getName(), EOL);
        System.out.format("\tMessaggio dell'eccezione: %s%s", exc.getMessage(), EOL);
    }

    synchronized public void logCritical(Error exc, String errorTemplate, String ...params) {
        if(level.getValue() < LoggerLevel.ERROR.getValue()) {
            return;
        }
        System.out.print("[CRITICAL] ");
        System.out.format(errorTemplate, params);
        System.out.format("%s\tMessaggio d'errore: %s%s", EOL, exc.getMessage(), EOL);
        exc.printStackTrace(System.out);
    }

}
