package org.lucacanella.csvdiviner.Core;

import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.io.Reader;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.GsonBuilder;
import com.google.gson.Gson;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

public class CsvDiviner {

    private long elapsedNanotime;

    public static final String VERSION = "1.1.1";

    private DivinerConfig.LoggerLevel loggerState = DivinerConfig.LoggerLevel.ERROR;

    /** Output: "Headers" */
    private String outHeaders;

    /** Output: "Count" */
    private Integer outRowCount;

    private FieldAnalysis[] fields;

    private static final String EOL = System.lineSeparator();

    private boolean trimWhitespace;

    public CsvParserSettings getCsvParserSettings() {
        return parserSettings;
    }

    private CsvParserSettings parserSettings;

    DivinerConfig cfg;

    private int currentWorkerCount;

    private int currentBatchSize;

    private Map<String, Integer> lastRunWorkerThreadSleepCount;
    private Map<String, Integer> lastRunWorkerThreadWakeCount;

    public CsvDiviner(DivinerConfig cfg) {
        this.cfg = cfg;
        parserSettings = new CsvParserSettings();
        parserSettings.getFormat().setDelimiter(cfg.getSeparator());
        parserSettings.getFormat().setQuote(cfg.getQuoteChar());
        parserSettings.getFormat().setQuoteEscape(cfg.getQuoteEscapeChar());
        parserSettings.setReadInputOnSeparateThread(cfg.getReadOnSeparateThread());
    }

    public DivinerConfig getConfiguration() {
        return cfg;
    }

    public String getHeaders() {
        return outHeaders;
    }

    public long getElapsedNanotime() {
        return elapsedNanotime;
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
        currentBatchSize = cfg.getBatchSize();
        currentWorkerCount = cfg.getWorkersCount();

        elapsedNanotime = 0;
        Reader reader;

        Thread[] evthreads;
        EvaluatorThread[] evths = null;
        ReaderThread r = null;
        boolean gotError = false;
        Path inputPath = Paths.get(filePathStr);


        lastRunWorkerThreadSleepCount = new HashMap<>();
        lastRunWorkerThreadWakeCount = new HashMap<>();

        try {
            long evaluateStart = System.nanoTime();
            reader = Files.newBufferedReader(inputPath, Charset.forName(cfg.getEncoding()));

            CsvParser parser = new CsvParser(parserSettings);
            parser.beginParsing(reader);

            String[] headers = parser.parseNext();
            this.outHeaders = String.join(", ", headers);

            evths = new EvaluatorThread[currentWorkerCount];
            evthreads = new Thread[currentWorkerCount];
            for (int thi = 0; thi < evths.length; ++thi) {
                evths[thi] = new EvaluatorThread(thi, this, headers);
                evths[thi].setTrimWhitespace(trimWhitespace);
                evthreads[thi] = new Thread(evths[thi]);
                evthreads[thi].start();
            }

            r = new ReaderThread(this, parser, evths, cfg.stateListener);
            Thread th = new Thread(r);
            th.start();

            th.join();
            for (int thi = 0; thi < evths.length; ++thi) {
                this.logInfo(String.format("Attesa completamento worker %d", thi));
                evthreads[thi].join();
                lastRunWorkerThreadSleepCount.put(evthreads[thi].getName(), evths[thi].getTotalSleepCount());
                lastRunWorkerThreadWakeCount.put(evthreads[thi].getName(), evths[thi].getTotalWakeCount());
            }
            EvaluatorThread ev0 = evths[0];
            for (int thi = 1; thi < evths.length; ++thi) {
                ev0.mergeFieldTypes(evths[thi]);
            }
            outRowCount = r.getRowCount();

            fields = ev0.finalizeAndGetFields();
            elapsedNanotime = System.nanoTime() - evaluateStart;

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

    synchronized public void logWarn(String warningTemplate, Object ...params) {
        if(loggerState.getValue() < DivinerConfig.LoggerLevel.WARNING.getValue()) {
            return;
        }
        System.out.print("[Warning] ");
        System.out.format(warningTemplate, params);
        System.out.print(EOL);
    }
    synchronized public void logInfo(String infoTemplate, Object ...params) {
        if(loggerState.getValue() < DivinerConfig.LoggerLevel.INFO.getValue()) {
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
        if(loggerState.getValue() < DivinerConfig.LoggerLevel.ERROR.getValue()) {
            return;
        }
        System.out.print("[Error] ");
        System.out.format(errorTemplate, params);
        System.out.format("%s\tTipo eccezione: %s%s", EOL, exc.getClass().getName(), EOL);
        System.out.format("\tMessaggio dell'eccezione: %s%s", exc.getMessage(), EOL);
    }
    synchronized public void logCritical(Error exc, String errorTemplate, String ...params) {
        if(loggerState.getValue() < DivinerConfig.LoggerLevel.ERROR.getValue()) {
            return;
        }
        System.out.print("[CRITICAL] ");
        System.out.format(errorTemplate, params);
        System.out.format("%s\tMessaggio d'errore: %s%s", EOL, exc.getMessage(), EOL);
        exc.printStackTrace(System.out);
    }

    synchronized public int getBatchSize() {
        return currentBatchSize;
    }

    synchronized public int getWorkersCount() {
        return currentWorkerCount;
    }

    public void printExecutionStats(PrintStream out) {
        for(String k : lastRunWorkerThreadWakeCount.keySet()) {
            out.println(String.format("Worker %s stopped %d times and woke %d times.",
                    k,
                    lastRunWorkerThreadSleepCount.get(k).intValue(),
                    lastRunWorkerThreadWakeCount.get(k).intValue()
            ));
        }
    }

}
