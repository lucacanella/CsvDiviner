package org.lucacanella.csvdiviner.Core;

import com.univocity.parsers.csv.CsvParser;

import java.util.concurrent.atomic.AtomicBoolean;

class ReaderThread implements Runnable {

    protected int count;

    EvaluatorThread[] evths;

    CsvDiviner parent;

    AtomicBoolean abort;

    CsvParser csvParser;

    private int batchSize;

    private int workersCount;

    private int lastUsedThread;

    private DivinerConfig.ReadStateListener stateListener;

    public ReaderThread(CsvDiviner parent, CsvParser csvParser, EvaluatorThread[] evths,
                        DivinerConfig.ReadStateListener listener) {
        this.parent = parent;
        this.csvParser = csvParser;
        this.evths = evths;
        this.abort = new AtomicBoolean(false);
        this.batchSize = parent.getBatchSize();
        this.workersCount = parent.getWorkersCount();
        this.stateListener = listener;
    }

    private void sendDataEndToAll() {
        for(int i = 0; i < evths.length; ++i) {
            evths[i].notifyDataEnd();
        }
    }

    public void abort() {
        parent.logWarn("Reader got abort signal.");
        this.abort.set(true);
    }

    public void setStateListener(DivinerConfig.ReadStateListener stateListener) {
        this.stateListener = stateListener;
    }

    @Override
    public void run() {
        String[] nl;
        parent.logInfo("Inizio lettura");
        int batchCount;
        count = 0;
        lastUsedThread = 0;
        stateListener.onBatchRead(0);
        do {
            EvaluatorThread worker = findNextIdleWorkerOrWait();
            worker.lockBuffer();
            String[][] buffData = new String[batchSize][];
            batchCount = 0;
            for (; batchCount < batchSize && null != (nl = csvParser.parseNext()); ++batchCount) {
                buffData[batchCount] = nl;
                ++count;
                if (abort.get()) {
                    parent.logInfo("Richiesto annullamento procedura. Stop!");
                    break;
                }
            }
            parent.logInfo(
                    String.format("Letti %d/%d record in questo batch (%d letti in totale). Sveglio il worker %d",
                            batchCount, batchSize, count, worker.getId()
                    )
            );
            worker.addRows(buffData, count, batchCount);
            worker.unlockBuffer();
            worker.notifyDataReady();
            stateListener.onBatchRead(count);
        } while (batchCount > 0);
        parent.logInfo("Fine lettura file");
        sendDataEndToAll();
        stateListener.onReadEnd(count);
    }

    private EvaluatorThread findNextIdleWorkerOrWait() {
        int buffId = lastUsedThread;
        EvaluatorThread worker = null;
        try {
            for (int retry = 0; retry < workersCount && !evths[buffId].waitsForData(); ++retry) {
                buffId = (1 + buffId) % evths.length;
            }
            worker = evths[buffId];
            lastUsedThread = buffId;
            parent.logInfo(String.format("Worker %d selezionato.", worker.getId()));
        } catch (Exception exc) {
            parent.logError(exc, "Errore durante la ricerca o l'attesa di un buffer in stato idle.");
        }
        return worker;
    }

    public int getRowCount() {
        return count;
    }

}
