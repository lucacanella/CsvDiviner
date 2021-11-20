package org.lucacanella.csvdiviner.Core.CsvDiviner;

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
import org.lucacanella.csvdiviner.Core.Config.CsvDivinerConfig;
import org.lucacanella.csvdiviner.Core.Log.DivinerLoggerInterface;
import org.lucacanella.csvdiviner.Core.Log.SilentLogger;
import org.lucacanella.csvdiviner.Core.Log.SynchronizedLogger;

public class CsvDiviner {

    private long elapsedNanotime;

    public static final String VERSION = "1.1.1";

    /** Output: "Headers" */
    private String outHeaders;

    /** Output: "Count" */
    private Integer outRowCount;

    private FieldAnalysis[] fields;

    private boolean trimWhitespace;

    public CsvParserSettings getCsvParserSettings() {
        return parserSettings;
    }

    private CsvParserSettings parserSettings;

    CsvDivinerConfig cfg;

    private int currentWorkerCount;

    private int currentBatchSize;

    private Map<String, Integer> lastRunWorkerThreadSleepCount;
    private Map<String, Integer> lastRunWorkerThreadWakeCount;

    DivinerLoggerInterface log;

    public CsvDiviner(CsvDivinerConfig cfg) {
        this(cfg, new SynchronizedLogger(cfg.getLoggerLevel()));
    }

    public CsvDiviner(CsvDivinerConfig cfg, DivinerLoggerInterface logger) {
        this.cfg = cfg;
        this.log = logger;
        parserSettings = new CsvParserSettings();
        parserSettings.getFormat().setDelimiter(cfg.getSeparator());
        parserSettings.getFormat().setQuote(cfg.getQuoteChar());
        parserSettings.getFormat().setQuoteEscape(cfg.getQuoteEscapeChar());
        parserSettings.setReadInputOnSeparateThread(cfg.getReadOnSeparateThread());
    }

    public CsvDivinerConfig getConfiguration() {
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
                log.logInfo(String.format("Attesa completamento worker %d", thi));
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
            log.logError(noFileExc,"Il file di input specificato non esiste @ %s", inputPath.toAbsolutePath().toString());
        } catch (Exception exc) {
            gotError = true;
            log.logError(exc,  "Errore lettura file");
        } catch (Error err) {
            gotError = true;
            log.logCritical(err, "Errore critico.");
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

    public DivinerLoggerInterface getLogger() {
        return log;
    }

    public void setLogger(SilentLogger silentLogger) {
        this.log = silentLogger;
    }

}
