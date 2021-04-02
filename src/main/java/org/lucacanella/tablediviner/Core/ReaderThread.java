package org.lucacanella.tablediviner.Core;

import com.univocity.parsers.csv.CsvParser;

import java.util.concurrent.atomic.AtomicBoolean;

class ReaderThread implements Runnable {

    protected int count;

    EvaluatorThread[] evths;

    TableDiviner parent;

    AtomicBoolean abort;

    CsvParser csvParser;

    private int batchSize;
    private int workersCount;

    public ReaderThread(TableDiviner parent, CsvParser csvParser, EvaluatorThread[] evths) {
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
        for(int i = 0; i < evths.length; ++i) {
            evths[i].wake(0);
        }
    }

    public void abort() {
        parent.logWarn("Reader got abort signal.");
        this.abort.set(true);
    }

    @Override
    public void run() {
        String[] nl;
        try {
            int bufid = 0;
            parent.logInfo("Inizio lettura");
            while(null != (nl = csvParser.parseNext())) {
                for(int retry = 0
                    ; retry < workersCount && evths[bufid].isProcessingData()
                        ; ++retry) {
                    bufid = (1+bufid) % evths.length;
                    Thread.sleep(5);
                }
                if(evths[bufid].isProcessingData()) {
                    parent.logInfo(String.format("I worker sono ancora al lavoro (ultimo controllo su %d che sta ancora lavorando). In attesa.", bufid));
                    evths[bufid].waitProcessingEnd();
                }
                evths[bufid].addRow(nl);
                ++count;
                if(0 == (count % batchSize)) {
                    parent.logInfo(String.format("Letti %d record (%d totali). Sveglio il worker %d", batchSize, count, bufid));
                    evths[bufid].wake(count);
                    bufid = (1+bufid) % evths.length;
                }
                if(abort.get()) {
                    break;
                }
            }
        } catch (Exception exc) {
            parent.logError(exc, "Errore durante la lettura del file.");
        }
        sendDataEndToAll();
        parent.logInfo("Fine lettura file");
    }

    public int getCount() {
        return count;
    }

}
