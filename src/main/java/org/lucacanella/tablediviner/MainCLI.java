package org.lucacanella.tablediviner;

import org.lucacanella.tablediviner.Core.TableDiviner;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MainCLI {

    public static final String VERSION = "0.2";

    @Parameter(description = "input_file")
    private List<String> inputFilePath = new ArrayList<>();

    @Parameter(names = { "-l", "--logger" }, description = "Set logger level (OFF|INFO|WARN|ERROR).")
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
    private int batchSize = 5000;

    @Parameter(names = { "-o", "--output" }, description = "If set, writes analysis json result in a file at the given path.")
    private String outputFilePath = null;

    @Parameter(names = { "-t", "--line-terminator" }, description = "Sets the line terminator (options are 'LF' or 'CRLF').")
    private String lineTerminator = null;

    @Parameter(names = { "-v", "--verbose" }, description = "Prints some information.")
    private boolean verboseMode = false;

    @Parameter(names = { "-c", "--charset" }, description = "Set input file encoding (defaults to UTF-8, see https://docs.oracle.com/javase/8/docs/technotes/guides/intl/encoding.doc.html).")
    private String encoding = "UTF-8";

    @Parameter(names = { "--version" }, description = "Prints version information.")
    private boolean versionMode = false;

    @Parameter(names = { "--silent" }, description = "Sets silent mode (prints only logs).")
    private boolean silentMode = false;

    public static void main(String[] args) {
        var main = new MainCLI();
        JCommander command = JCommander.newBuilder()
                .addObject(main)
                .build();
        command.parse(args);

        if(main.versionMode) {
            System.out.format("TableDiviner versione: %s%sCli Versione: %s%s",
                    TableDiviner.VERSION, System.lineSeparator(), MainCLI.VERSION, System.lineSeparator());
        }

        if(main.separator.length() != 1) {
            System.out.format("Il parametro separatore (-s) deve essere un singolo carattere (\"%s\" ricevuto).%s", main.separator, System.lineSeparator());
        } else if(main.quoteChar.length() != 1) {
            System.out.format("Il qualificatore di stringa (-q) deve essere un singolo carattere (\"%s\" ricevuto).%s", main.quoteChar, System.lineSeparator());
        } else if(main.escapeChar.length() != 1) {
            System.out.format("Il carattere di escape (-e) deve essere un singolo carattere (\"%s\" ricevuto).%s", main.escapeChar, System.lineSeparator());
        } else if(main.inputFilePath.size() != 1) {
            System.out.println("Specifica uno ed un solo file di input come argomento.");
        } else if (main.lineTerminator != null && !(main.lineTerminator.equals("LF") || main.lineTerminator.equals("CRLF"))) {
            System.out.println("Terminatore di linea non supportato. Solamente \"LF\" o \"CRLF\" sono supportati supported (ie. -t CRLF)");
        } else {
            String inputFilePath = main.inputFilePath.get(0);
            if(!main.silentMode) {
                System.out.format("Input file: %s%s", inputFilePath, System.lineSeparator());
            }
            TableDiviner diviner = new TableDiviner(
                    main.separator.charAt(0),
                    main.quoteChar.charAt(0),
                    main.escapeChar.charAt(0),
                    main.encoding,
                    main.batchSize, main.workerThreadsCount
            );
            if(main.lineTerminator != null) {
                if(main.lineTerminator.equals("LF")) {
                    diviner.getCsvParserSettings().getFormat().setLineSeparator("\n");
                } else if (main.lineTerminator.equals("CRLF")) {
                    diviner.getCsvParserSettings().getFormat().setLineSeparator("\r\n");
                }
            }

            if(main.verboseMode) {
                System.out.format("Csv parser settings: %s%s", diviner.getCsvParserSettings(), System.lineSeparator());
            }
            diviner.setLoggerState(TableDiviner.LoggerState.valueOf(main.loggerState));
            diviner.evaluateFile(inputFilePath);
            if(main.outputFilePath != null) {
                BufferedWriter writer = null;
                try {
                    writer = new BufferedWriter(new FileWriter(main.outputFilePath));
                    writer.write(diviner.getFieldTypesJson());
                    writer.close();
                    if(!main.silentMode) {
                        System.out.format("\tL'output è stato scritto nel file %s%s", main.outputFilePath, System.lineSeparator());
                    }
                } catch (IOException e) {
                    System.out.println("Errore imprevisto durante la scrittura del file di output:");
                    System.out.format("\t%s", e.getMessage(), System.lineSeparator());
                }
            } else {
                System.out.println("Risultato dell'analisi: ");
                System.out.println(diviner.getFieldTypesJson());
            }
            if(!main.silentMode) {
                System.out.format("%,d righe analizzate nel file%s", diviner.getRowCount(), System.lineSeparator());
            }
        }

        if(main.helpUsage) {
            command.usage();
            return;
        }
    }

}
