package org.lucacanella.csvdiviner.Core.CsvDiviner;

import org.lucacanella.csvdiviner.Core.Log.DivinerLoggerInterface;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

class EvaluatorThread implements Runnable {

    CsvDiviner parent;
    int idx;
    int currentRowOffset;

    DivinerLoggerInterface log;

    AtomicBoolean isWaitingForData;
    AtomicBoolean dataEnd;
    ReentrantLock bufferMutex;
    Object waitForData;

    String[][] rowBuffer;
    int rowsInBuffer;

    FieldAnalysis[] fields;
    int fieldsCount;

    private int batchSize;
    private int workersCount;
    private boolean trimWhitespace;

    private int sleepCount;
    private int wakeCount;

    public FieldAnalysis[] getFields() {
        return fields;
    }

    public FieldAnalysis[] finalizeAndGetFields() {
        Arrays.stream(fields).forEach(cf -> { cf.finalizeBeforeSerialize(); });
        return fields;
    }

    public EvaluatorThread(int idx, CsvDiviner parent, String[] headers) {
        this.log = parent.getLogger();
        this.idx = idx;
        this.parent = parent;
        this.fieldsCount = headers.length;
        this.wakeCount = 0;

        currentRowOffset = 0;
        trimWhitespace = true;
        rowBuffer = null;
        fields = new FieldAnalysis[fieldsCount];
        for(int i = 0; i < fieldsCount; ++i) {
            fields[i] = new FieldAnalysis(headers[i], i);
        }
        rowsInBuffer = 0;
        dataEnd = new AtomicBoolean(false);
        isWaitingForData = new AtomicBoolean(true);
        waitForData = new Object();
        bufferMutex = new ReentrantLock();
        batchSize = parent.getBatchSize();
        workersCount = parent.getWorkersCount();
    }


    public boolean waitsForData() {
        return isWaitingForData.get();
    }

    public void addRows(String[][] rows, int currentOffset, int rowsInBuffer) {
        rowBuffer = rows;
        currentRowOffset = currentOffset - rowsInBuffer;
        this.rowsInBuffer = rowsInBuffer;
    }

    public void unlockBuffer() {
        log.logInfo("Sblocco mutex del buffer per worker %d", idx, currentRowOffset);
        this.bufferMutex.unlock();
    }

    public void notifyDataReady() {
        synchronized (waitForData) {
            this.isWaitingForData.set(false);
            this.waitForData.notify();
        }
        log.logInfo("Sblocco attesa dati per worker %d", idx, currentRowOffset);
    }

    public void notifyDataEnd() {
        log.logInfo("Notifica fine dati ricevuta per worker %d", idx, currentRowOffset);
        this.dataEnd.set(true);
        synchronized (waitForData) {
            if (isWaitingForData.get()) {
                this.waitForData.notify();
                log.logInfo("Sblocco attesa dati per worker %d", idx, currentRowOffset);
            }
        }
    }

    public void abort() {
        log.logWarn("Il worker %d ha ricevuto un segnale di annullamento.", this.idx);
        this.dataEnd.set(true);
    }

    @Override
    public void run() {
        try {
            while(!dataEnd.get()) {
                synchronized (waitForData) {
                    if (isWaitingForData.get()) {
                        log.logInfo(String.format("Worker %d attende dati...", idx));
                        ++sleepCount;
                        waitForData.wait();
                    } else {
                        log.logInfo(String.format("Worker %d contiene %d righe di dati.", idx, rowsInBuffer));
                        processData();
                    }
                }
            }
        } catch (InterruptedException exc) {
            this.notifyDataEnd();
            log.logError(exc, "Errore in thread di valutazione: %s", exc.getMessage());
        }
        log.logInfo("Worker %d termina esecuzione.", idx);
    }

    private void processData() {
        ++wakeCount;
        try {
            bufferMutex.lock();
            log.logInfo(String.format("Worker %d inizia a processare i dati.", idx));
            int fieldsCount = fields.length;
            int i;
            for (i = 0; rowsInBuffer > 0; ++i) {
                //check fields
                String[] rowData = rowBuffer[i];
                if (null == rowData) {
                    log.logWarn("Riga nulla @ %d", currentRowOffset + i + 1);
                } else if (rowData.length != fields.length) {
                    if (rowData.length < 1 || (rowData.length < 2 && rowData[0].length() < 1)) {
                        log.logWarn("Riga vuota @ %d", currentRowOffset + i + 1);
                    } else {
                        log.logWarn(
                                String.format("Numero di campi inconsistente tra i record @ %d, n colonne %d/%d", currentRowOffset + i + 1, rowData.length, fieldsCount)
                        );
                    }
                } else {
                    for (int j = 0; j < fieldsCount; ++j) {
                        if (trimWhitespace && null != rowData[j]) {
                            fields[j].evaluate(rowData[j].trim(), j);
                        } else {
                            fields[j].evaluate(rowData[j], j);
                        }
                    }
                }
                //clear row
                rowBuffer[i] = null;
                --rowsInBuffer;
                isWaitingForData.set(true);
            }
            log.logInfo(String.format("Worker %d ha processato %d righe di dati.", idx, i));
        } catch (Exception e) {
            this.notifyDataEnd();
            log.logErrorWithStacktrace(e, "Errore generico nel thread di valutazione (per worker %d): %s", e.getMessage());
        } finally {
            log.logInfo(String.format("Sblocco del mutex per worker %d a termine elaborazione dati.", idx));
            bufferMutex.unlock();
        }
    }

    public void mergeFieldTypes(EvaluatorThread other) {
        for(int i = 0; i < fields.length; ++i) {
            fields[i].mergeType(other.fields[i]);
        }
    }

    public void setTrimWhitespace(boolean trimWhitespace) {
        this.trimWhitespace = trimWhitespace;
    }

    public void lockBuffer() {
        this.bufferMutex.lock();
        log.logInfo("Blocco mutex del buffer per worker %d per inizio scrittura dati)", idx, currentRowOffset);
    }

    public int getId() {
        return idx;
    }

    public int getTotalWakeCount() {
        return wakeCount;
    }

    public int getTotalSleepCount() {
        return sleepCount;
    }

}
