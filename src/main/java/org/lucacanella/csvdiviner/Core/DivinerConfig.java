package org.lucacanella.csvdiviner.Core;

import com.univocity.parsers.csv.CsvParserSettings;

public class DivinerConfig {

    public final static int DEFAULT_BATCH_SIZE = 10000;

    private final char separator;
    private final char quoteChar;
    private final char quoteEscapeChar;
    private final boolean readOnSeparateThread = true;

    private final int batchSize;
    private final int workersCount;
    private final String encoding;
    private final boolean trimWhitespace;

    public interface ReadStateListener {
        void onBatchRead(int rowsRead);
        void onReadEnd(int rowCount);
    }

    ReadStateListener stateListener = new ReadStateListener() {
        @Override
        public void onBatchRead(int rowsRead) {}
        @Override
        public void onReadEnd(int rowCount) {}
    };

    private LoggerLevel loggerLevel;

    private CsvParserSettings parserSettings;

    public String getEncoding() {
        return encoding;
    }

    public char getQuoteChar() {
        return quoteChar;
    }

    public char getSeparator() {
        return separator;
    }

    public char getQuoteEscapeChar() {
        return quoteEscapeChar;
    }

    public boolean getReadOnSeparateThread() {
        return readOnSeparateThread;
    }

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

    public DivinerConfig(char separator) {
        this(separator, '"');
    }

    public DivinerConfig(char separator, char quoteChar) {
        this(separator, quoteChar, '\\');
    }

    public DivinerConfig(char separator, char quoteChar, char escapeChar) {
        this(separator, quoteChar, escapeChar, "UTF-8", DivinerConfig.DEFAULT_BATCH_SIZE, 7, LoggerLevel.ERROR);
    }

    public DivinerConfig(char separator, char quoteChar, char escapeChar, String encoding) {
        this(separator, quoteChar, escapeChar, encoding, DivinerConfig.DEFAULT_BATCH_SIZE, 7, LoggerLevel.ERROR);
    }

    public DivinerConfig(
            char separator, char quoteChar, char escapeChar, String encoding, int batchSize, int workersCount) {
        this(separator, quoteChar, escapeChar, encoding, batchSize, workersCount, LoggerLevel.ERROR);
    }

    public DivinerConfig (
            char separator, char quoteChar, char escapeChar, String encoding,
            int batchSize, int workersCount,
            LoggerLevel loggerLevel) {

        this.setLoggerLevel(loggerLevel);

        this.separator = separator;
        this.quoteChar = quoteChar;
        this.batchSize = batchSize;
        this.quoteEscapeChar = escapeChar;
        this.workersCount = workersCount;
        this.encoding = encoding;
        this.trimWhitespace = true;

        parserSettings = new CsvParserSettings();
        parserSettings.getFormat().setDelimiter(separator);
        parserSettings.getFormat().setQuote(quoteChar);
        parserSettings.getFormat().setQuoteEscape(escapeChar);
    }

    public void setStateListener(DivinerConfig.ReadStateListener stateListener) {
        this.stateListener = stateListener;
    }

    public void setLoggerLevel(LoggerLevel loggerLevel) {
        this.loggerLevel = loggerLevel;
    }

    public CsvParserSettings getParserSettings() {
        return parserSettings;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getWorkersCount() {
        return workersCount;
    }

}
