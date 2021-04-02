package org.lucacanella.tablediviner.Core;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

class EvaluatorThread implements Runnable {

    int idx;

    String[][] rowBuffer;
    int rowsInBuffer;
    AtomicBoolean dataEnd;

    FieldAnalysis[] fields;
    int fieldsCount;

    TableDiviner parent;

    private int batchSize;
    private int workersCount;
    private boolean trimWhitespace;

    public FieldAnalysis[] getFields() {
        return fields;
    }

    public FieldAnalysis[] finalizeAndGetFields() {
        Arrays.stream(fields).forEach(cf -> { cf.finalizeBeforeSerialize(); });
        return fields;
    }

    Object bufferSemaphore;
    Object bufferWriteSemaphore;
    AtomicBoolean processingData;

    int currentRowOffset;

    public EvaluatorThread(int idx, TableDiviner parent, String[] headers) {
        this.currentRowOffset = 0;
        this.trimWhitespace = true;
        this.idx = idx;
        this.parent = parent;
        this.fieldsCount = headers.length;
        this.rowBuffer = new String[parent.getBatchSize()][fieldsCount];
        this.fields = new FieldAnalysis[fieldsCount];
        for(int i = 0; i < fieldsCount; ++i) {
            fields[i] = new FieldAnalysis(headers[i], i);
        }
        rowsInBuffer = 0;
        dataEnd = new AtomicBoolean(false);
        processingData = new AtomicBoolean(false);
        bufferSemaphore = new Object();
        bufferWriteSemaphore = new Object();
        this.batchSize = parent.getBatchSize();
        this.workersCount = parent.getWorkersCount();
    }


    public boolean isProcessingData() {
        return processingData.get();
    }

    public void waitProcessingEnd() {
        synchronized(bufferWriteSemaphore) {
            try {
                bufferWriteSemaphore.wait(3_000);
            } catch (InterruptedException exc) {
                parent.logError(exc, "Errore durante l'attesa di processamento dati");
            }
        }
    }

    public void addRow(String[] row) {
        rowBuffer[this.rowsInBuffer++] = row;
    }

    synchronized public void wake(int currentOffset) {
        synchronized (bufferSemaphore) {
            this.bufferSemaphore.notify();
            this.currentRowOffset = currentOffset;
            parent.logInfo("Worker %d sveglio @ off %d", idx, currentOffset);
            if(!dataEnd.get()) {
                processingData.set(true);
            }
        }
    }

    public void notifyDataEnd() {
        this.dataEnd.set(true);
    }

    public void abort() {
        parent.logWarn("Il worker %d ha ricevuto un segnale di annullamento.", this.idx);
        this.dataEnd.set(true);
    }

    @Override
    public void run() {
        try {
            int fieldsCount = fields.length;
            while(!dataEnd.get()) {
                synchronized (bufferSemaphore) {
                    parent.logInfo(String.format("%d attende dati...", idx));
                    bufferSemaphore.wait(5_000);
                }
                parent.logInfo(String.format("%d riceve %d righe di dati.", idx, rowsInBuffer));
                int i;
                for(i = 0; rowsInBuffer > 0; ++i) {
                    //check fields
                    String[] rowData = rowBuffer[i];
                    if(null == rowData) {
                        this.parent.logWarn("Riga nulla @ %d", currentRowOffset+i+1);
                    } else if(rowData.length != fields.length) {
                        if(rowData.length < 1 || (rowData.length < 2 && rowData[0].length() < 1)) {
                            this.parent.logWarn("Riga vuota @ %d", currentRowOffset+i+1);
                        } else {
                            parent.logWarn(
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
                }
                parent.logInfo(String.format("%d ha processato %d righe di dati.", idx, i));
                processingData.set(false);
            }
        } catch (InterruptedException exc) {
            this.notifyDataEnd();
            parent.logError(exc, "Errore in thread di valutazione: %s", exc.getMessage());
        } catch (Exception e) {
            this.notifyDataEnd();
            parent.logErrorWithStacktrace(e, "Errore generico nel thread di valutazione: %s", e.getMessage());
        }
        parent.logInfo("%d termina esecuzione.", idx);
    }

    public void mergeFieldTypes(EvaluatorThread other) {
        for(int i = 0; i < fields.length; ++i) {
            fields[i].mergeType(other.fields[i]);
        }
    }

    public void setTrimWhitespace(boolean trimWhitespace) {
        this.trimWhitespace = trimWhitespace;
    }
}
