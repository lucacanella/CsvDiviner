package org.lucacanella.csvdiviner.Core;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.io.Reader;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.gson.GsonBuilder;
import com.google.gson.Gson;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

public class CsvDiviner {

    private String encoding;

    public enum LoggerState {
        OFF(4),
        INFO(3),
        WARNING(2),
        ERROR(1);

        private final int value;

        LoggerState(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    public static final String VERSION = "0.2";

    private LoggerState loggerState = LoggerState.ERROR;

    /** Output: "Headers" */
    private String outHeaders;

    /** Output: "Count" */
    private Integer outRowCount;

    private FieldAnalysis[] fields;

    private int batchSize;

    private int workersCount;

    private static final String EOL = System.lineSeparator();

    private char separator;

    private char quoteChar;

    private char escapeChar;

    private boolean trimWhitespace;

    public CsvParserSettings getCsvParserSettings() {
        return parserSettings;
    }

    private CsvParserSettings parserSettings;

    public CsvDiviner(char separator) {
        this(separator, '"');
    }

    public CsvDiviner(char separator, char quoteChar) {
        this(separator, quoteChar, '\\');
    }

    public CsvDiviner(char separator, char quoteChar, char escapeChar) {
        this(separator, quoteChar, escapeChar, "UTF-8", 5000, 7);
    }

    public CsvDiviner(char separator, char quoteChar, char escapeChar, String encoding) {
        this(separator, quoteChar, escapeChar, encoding, 5000, 7);
    }

    public CsvDiviner(char separator, char quoteChar, char escapeChar, String encoding, int batchSize, int workersCount) {
        this.separator = separator;
        this.quoteChar = quoteChar;
        this.batchSize = batchSize;
        this.escapeChar = escapeChar;
        this.workersCount = workersCount;
        this.encoding = encoding;
        this.trimWhitespace = true;

        parserSettings = new CsvParserSettings();
        parserSettings.getFormat().setDelimiter(separator);
        parserSettings.getFormat().setQuote(quoteChar);
        parserSettings.getFormat().setQuoteEscape(escapeChar);
    }

    public void setTrimWhitespace(boolean trimWhitespace) {
        this.trimWhitespace = trimWhitespace;
    }

    public String getHeaders() {
        return outHeaders;
    }

    public Integer getRowCount() {
        return outRowCount;
    }

    public FieldAnalysis[] getFields() {
        return fields;
    }

    public String getFieldTypesJson() {
        Gson gson = new GsonBuilder()
                .setPrettyPrinting()
                .create();
        return gson.toJson(fields);
    }

    public void evaluateFile(String filePathStr) {
        Reader reader;

        Thread[] evthreads;
        EvaluatorThread[] evths = null;
        ReaderThread r = null;
        boolean gotError = false;
        Path inputPath = Paths.get(filePathStr);

        try {
            reader = Files.newBufferedReader(inputPath, Charset.forName(encoding));

            CsvParser parser = new CsvParser(parserSettings);
            parser.beginParsing(reader);

            String[] headers = parser.parseNext();
            this.outHeaders = String.join(", ", headers);

            evths = new EvaluatorThread[workersCount];
            evthreads = new Thread[workersCount];
            for (int thi = 0; thi < evths.length; ++thi) {
                evths[thi] = new EvaluatorThread(thi, this, headers);
                evths[thi].setTrimWhitespace(trimWhitespace);
                evthreads[thi] = new Thread(evths[thi]);
                evthreads[thi].start();
            }

            r = new ReaderThread(this, parser, evths);
            Thread th = new Thread(r);
            th.start();

            th.join();
            for (int thi = 0; thi < evths.length; ++thi) {
                evthreads[thi].join();
            }
            EvaluatorThread ev0 = evths[0];
            for (int thi = 1; thi < evths.length; ++thi) {
                ev0.mergeFieldTypes(evths[thi]);
            }
            outRowCount = r.getCount();

            fields = ev0.finalizeAndGetFields();

        } catch (NoSuchFileException noFileExc) {
            gotError = true;
            logError(noFileExc,"Il file di input specificato non esiste @ %s", inputPath.toAbsolutePath().toString());
        } catch (Exception exc) {
            gotError = true;
            logError(exc,  "Errore lettura file");
        } catch (Error err) {
            gotError = true;
            logCritical(err, "Errore critico.");
        } finally {
            if(gotError) {
                abort(evths, r);
            }
        }
    }

    public void abort(EvaluatorThread[] evths, ReaderThread r) {
        if (evths != null) {
            for (EvaluatorThread evth : evths) {
                evth.abort();
            }
        }
        if (r != null) {
            r.abort();
        }
    }

    synchronized public void setLoggerState(LoggerState loggerState) {
        this.loggerState = loggerState;
    }

    synchronized public void logWarn(String warningTemplate, Object ...params) {
        if(loggerState.getValue() < LoggerState.WARNING.getValue()) {
            return;
        }
        System.out.print("[Warning] ");
        System.out.format(warningTemplate, params);
        System.out.print(EOL);
    }
    synchronized public void logInfo(String infoTemplate, Object ...params) {
        if(loggerState.getValue() < LoggerState.INFO.getValue()) {
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
        if(loggerState.getValue() < LoggerState.ERROR.getValue()) {
            return;
        }
        System.out.print("[Error] ");
        System.out.format(errorTemplate, params);
        System.out.format("%s\tTipo eccezione: %s%s", EOL, exc.getClass().getName(), EOL);
        System.out.format("\tMessaggio dell'eccezione: %s%s", exc.getMessage(), EOL);
    }
    synchronized public void logCritical(Error exc, String errorTemplate, String ...params) {
        if(loggerState.getValue() < LoggerState.ERROR.getValue()) {
            return;
        }
        System.out.print("[CRITICAL] ");
        System.out.format(errorTemplate, params);
        System.out.format("%s\tMessaggio d'errore: %s%s", EOL, exc.getMessage(), EOL);
        exc.printStackTrace(System.out);
    }

    synchronized public int getBatchSize() {
        return batchSize;
    }

    synchronized public int getWorkersCount() {
        return workersCount;
    }
}
