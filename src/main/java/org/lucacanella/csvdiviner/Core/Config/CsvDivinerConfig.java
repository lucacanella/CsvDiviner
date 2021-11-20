package org.lucacanella.csvdiviner.Core.Config;

import com.univocity.parsers.csv.CsvParserSettings;
import org.lucacanella.csvdiviner.Core.Log.LoggerLevel;

public class CsvDivinerConfig
        extends AbstractDivinerConfig {

    public final static int DEFAULT_BATCH_SIZE = 10000;

    private char separator;
    private char quoteChar;
    private char quoteEscapeChar;
    private boolean readOnSeparateThread = true;

    private int batchSize;
    private int workersCount;
    private String encoding;
    private boolean ignoreWhitespace;

    public interface ReadStateListener {
        void onBatchRead(int rowsRead);
        void onReadEnd(int rowCount);
    }

    public ReadStateListener stateListener = new ReadStateListener() {
        @Override
        public void onBatchRead(int rowsRead) {}
        @Override
        public void onReadEnd(int rowCount) {}
    };

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

    public CsvDivinerConfig() {
        this.parserSettings = new CsvParserSettings();
    }

    public CsvDivinerConfig(char separator) {
        this(separator, '"');
    }

    public CsvDivinerConfig(char separator, char quoteChar) {
        this(separator, quoteChar, '\\');
    }

    public CsvDivinerConfig(char separator, char quoteChar, char escapeChar) {
        this(separator, quoteChar, escapeChar, "UTF-8", CsvDivinerConfig.DEFAULT_BATCH_SIZE, 7, LoggerLevel.ERROR);
    }

    public CsvDivinerConfig(char separator, char quoteChar, char escapeChar, String encoding) {
        this(separator, quoteChar, escapeChar, encoding, CsvDivinerConfig.DEFAULT_BATCH_SIZE, 7, LoggerLevel.ERROR);
    }

    public CsvDivinerConfig(
            char separator, char quoteChar, char escapeChar, String encoding, int batchSize, int workersCount) {
        this(separator, quoteChar, escapeChar, encoding, batchSize, workersCount, LoggerLevel.ERROR);
    }

    public CsvDivinerConfig(
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
        this.ignoreWhitespace = true;

        parserSettings = new CsvParserSettings();
        parserSettings.getFormat().setDelimiter(separator);
        parserSettings.getFormat().setQuote(quoteChar);
        parserSettings.getFormat().setQuoteEscape(escapeChar);
        parserSettings.setIgnoreLeadingWhitespaces(this.ignoreWhitespace);
        parserSettings.setIgnoreTrailingWhitespaces(this.ignoreWhitespace);
    }

    public void setStateListener(CsvDivinerConfig.ReadStateListener stateListener) {
        this.stateListener = stateListener;
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

    public void setSeparator(char separator) {
        this.separator = separator;
    }

    public void setQuoteChar(char quoteChar) {
        this.quoteChar = quoteChar;
    }

    public void setQuoteEscapeChar(char quoteEscapeChar) {
        this.quoteEscapeChar = quoteEscapeChar;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void setWorkersCount(int workersCount) {
        this.workersCount = workersCount;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public boolean isIgnoreWhitespace() {
        return ignoreWhitespace;
    }

    public void setIgnoreWhitespace(boolean ignoreWhitespace) {
        this.ignoreWhitespace = ignoreWhitespace;
    }

    public LoggerLevel getLoggerLevel() {
        return loggerLevel;
    }

    public void setParserSettings(CsvParserSettings parserSettings) {
        this.parserSettings = parserSettings;
    }

}
