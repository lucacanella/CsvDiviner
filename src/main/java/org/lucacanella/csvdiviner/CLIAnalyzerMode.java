package org.lucacanella.csvdiviner;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.lucacanella.csvdiviner.Core.CsvDiviner.CsvDiviner;
import org.lucacanella.csvdiviner.Core.Config.CsvDivinerConfig;
import org.lucacanella.csvdiviner.Core.Log.LoggerLevel;
import org.lucacanella.csvdiviner.Core.Log.SilentLogger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class CLIAnalyzerMode
        implements DivinerCLIExecutionModeInterface {

    public static final String VERSION = "1.2";

    @Parameter(description = "input_file")
    private List<String> inputFilePath = new ArrayList<>();

    @Parameter(names = { "-l", "--logger" }, description = "Set logger level (OFF|INFO|WARNING|ERROR).")
    private String loggerState = "ERROR";

    @Parameter(names = { "-h", "--help-usage" }, description = "Print help.")
    private boolean helpUsage = false;

    @Parameter(names = { "-s", "--separator" }, description = "Csv fields separator, defaults to ','.")
    private String separator = ",";

    @Parameter(names = { "-q", "--quotechar" }, description = "Csv fields quote char - defaults to '\"'.")
    private String quoteChar = "\"";

    @Parameter(names = { "-e", "--escapechar" }, description = "Csv escape char - defaults to '\\'.")
    private String escapeChar = "\\";

    @Parameter(names = { "-w", "--worker-threads" }, description = "Number of worker threads.")
    private int workerThreadsCount = 7;

    @Parameter(names = { "-b", "--batch-size" }, description = "Batch size - how many rows must be read before being sent to worker thread.")
    private int batchSize = CsvDivinerConfig.DEFAULT_BATCH_SIZE;

    @Parameter(names = { "-o", "--output" }, description = "If set, writes analysis json result in a file at the given path.")
    private String outputFilePath = null;

    @Parameter(names = { "-t", "--line-terminator" }, description = "Sets the line terminator (options are 'LF', 'auto' or any other single character ).")
    private String lineTerminator = null;

    @Parameter(names = { "-i", "--ignoreWhitespace" }, description = "If true, ignores whitespace at the beginning and end of values. Defaults to false")
    private boolean ignoreWhitespace = false;

    @Parameter(names = { "-v", "--verbose" }, description = "Prints some information.")
    private boolean verboseMode = false;

    @Parameter(names = { "-c", "--charset" }, description = "Set input file encoding (defaults to UTF-8, see https://docs.oracle.com/javase/8/docs/technotes/guides/intl/encoding.doc.html).")
    private String encoding = "UTF-8";

    @Parameter(names = { "--version" }, description = "Prints version information.")
    private boolean versionMode = false;

    @Parameter(names = { "--silent" }, description = "Sets silent mode (prints only logs).")
    private boolean silentMode = false;

    @Parameter(names = { "--printStats" }, description = "Print execution stats")
    private boolean printExecutionStats = false;

    @Parameter(names = { "--showProgress" }, description = "Print execution stats")
    private boolean showProgress = false;

    public void execute(String[] cliArgs) {
        System.out.println(String.join("; ",cliArgs));
        JCommander command = JCommander.newBuilder()
            .addObject(this)
            .build();
        command.parse(cliArgs);

        if(this.versionMode) {
            System.out.format("CsvDiviner, Analyzer version: %s%sAnalyzer Cli Version: %s%s",
                    CsvDiviner.VERSION, System.lineSeparator(), CLIAnalyzerMode.VERSION, System.lineSeparator());
        }

        if(this.separator.length() != 1) {
            System.out.format("Il parametro separatore (-s) deve essere un singolo carattere (\"%s\" ricevuto).%s", this.separator, System.lineSeparator());
        } else if(this.quoteChar.length() != 1) {
            System.out.format("Il qualificatore di stringa (-q) deve essere un singolo carattere (\"%s\" ricevuto).%s", this.quoteChar, System.lineSeparator());
        } else if(this.escapeChar.length() != 1) {
            System.out.format("Il carattere di escape (-e) deve essere un singolo carattere (\"%s\" ricevuto).%s", this.escapeChar, System.lineSeparator());
        } else if(this.inputFilePath.size() != 1) {
            System.out.println("Specifica uno ed un solo file di input come argomento.");
        } else if (this.lineTerminator != null
                && !(
                        this.lineTerminator.equals("LF") || this.lineTerminator.equals("AUTO") || this.lineTerminator.length() == 1
        )) {
            System.out.println("Terminatore di linea non supportato. Sono supportate solamente le opzioni \"LF\", \"AUTO\" o un qualsiasi singolo carattere");
        } else {
            String inputFilePath = this.inputFilePath.get(0);
            CsvDivinerConfig divinerConfig = new CsvDivinerConfig(
                    this.separator.charAt(0),
                    this.quoteChar.charAt(0),
                    this.escapeChar.charAt(0),
                    this.encoding,
                    this.batchSize,
                    this.workerThreadsCount
            );
            if(!this.silentMode) {
                System.out.format("Input file: %s%s", inputFilePath, System.lineSeparator());
            }
            divinerConfig.setIgnoreWhitespace(this.ignoreWhitespace);
            divinerConfig.setLoggerLevel(LoggerLevel.valueOf(this.loggerState));
            if(this.showProgress) {
                System.out.println();
                divinerConfig.setStateListener(new DefaultProgressStateListener());
            }
            if(this.lineTerminator != null) {
                if(this.lineTerminator.equals("LF")) {
                    divinerConfig.getParserSettings().getFormat().setLineSeparator("\n");
                } else if (this.lineTerminator.equals("AUTO")) {
                    divinerConfig.getParserSettings().setDelimiterDetectionEnabled(true);
                } else {
                    divinerConfig.getParserSettings().getFormat().setLineSeparator(this.lineTerminator);
                }
            }
            CsvDiviner diviner = new CsvDiviner(divinerConfig);
            if(this.silentMode) {
                diviner.setLogger(new SilentLogger(LoggerLevel.ERROR));
            }
            if(this.verboseMode) {
                System.out.format("Csv parser settings: %s%s", diviner.getCsvParserSettings(), System.lineSeparator());
            }
            diviner.evaluateFile(inputFilePath);
            if(this.outputFilePath != null) {
                BufferedWriter writer = null;
                try {
                    writer = new BufferedWriter(new FileWriter(this.outputFilePath));
                    writer.write(diviner.getFieldTypesJson());
                    writer.close();
                    if(!this.silentMode) {
                        System.out.format("\tL'output è stato scritto nel file %s%s", this.outputFilePath, System.lineSeparator());
                    }
                } catch (IOException e) {
                    System.out.println("Errore imprevisto durante la scrittura del file di output:");
                    System.out.format("\t%s", e.getMessage(), System.lineSeparator());
                }
            } else {
                System.out.println("Risultato dell'analisi: ");
                System.out.println(diviner.getFieldTypesJson());
            }
            if(this.printExecutionStats || !this.silentMode) {
                long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(diviner.getElapsedNanotime());
                Runtime rt = Runtime.getRuntime();
                long totalMemoryMB = rt.totalMemory() / (1024 * 1024);
                long memoryUsedMB = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
                System.out.format("%,d righe analizzate in %d ms (%d/%d Mb ram utilizzati)%s",
                        diviner.getRowCount(), elapsedMillis, memoryUsedMB, totalMemoryMB, System.lineSeparator());
                if(this.printExecutionStats) {
                    diviner.printExecutionStats(System.out);
                }
            }
        }

        if(this.helpUsage) {
            command.usage();
            return;
        }
    }

    private class DefaultProgressStateListener
            implements CsvDivinerConfig.ReadStateListener {
        private int pCounter = 0;

        @Override
        public void onBatchRead(int rowsRead) {
            System.out.print(".");
            if(++pCounter % 10 == 0) {
                System.out.println();
            }
        }

        @Override
        public void onReadEnd(int rowCount) {
            System.out.println("|END|");
        }
    }
}
