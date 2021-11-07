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

    public ReaderThread(CsvDiviner parent, CsvParser csvParser, EvaluatorThread[] evths) {
        this.parent = parent;
        this.csvParser = csvParser;
        this.evths = evths;
        this.abort = new AtomicBoolean(false);
        this.batchSize = parent.getBatchSize();
        this.workersCount = parent.getWorkersCount();
    }

    private void sendDataEndToAll() {
        for(int i = 0; i < evths.length; ++i) {
            evths[i].notifyDataEnd();
        }
        /*for(int i = 0; i < evths.length; ++i) {
            evths[i].wake();
        }*/
    }

    public void abort() {
        parent.logWarn("Reader got abort signal.");
        this.abort.set(true);
    }

    @Override
    public void run() {
        String[] nl;
        parent.logInfo("Inizio lettura");
        int batchCount;
        count = 0;
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
        } while (batchCount > 0);
        parent.logInfo("Fine lettura file");
        sendDataEndToAll();
    }

    private EvaluatorThread findNextIdleWorkerOrWait() {
        int buffId = 0;
        EvaluatorThread worker = null;
        try {
            for (int retry = 0; retry < workersCount && !evths[buffId].waitsForData(); ++retry) {
                buffId = (1 + buffId) % evths.length;
                Thread.sleep(5);
            }
            worker = evths[buffId];
            parent.logInfo(String.format("Worker %d selezionato.", worker.getId()));
        } catch (InterruptedException exc) {
            parent.logError(exc, "Attesa interrotta durante ricerca di un worker in stato idle.");
        } catch (Exception exc) {
            parent.logError(exc, "Errore durante la ricerca o l'attesa di un buffer in stato idle.");
        }
        return worker;
    }

    public int getCount() {
        return count;
    }

}
