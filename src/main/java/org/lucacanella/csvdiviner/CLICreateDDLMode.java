package org.lucacanella.csvdiviner;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.lucacanella.csvdiviner.Core.Config.DataDefinitionDivinerConfig;
import org.lucacanella.csvdiviner.Core.CsvDiviner.CsvDiviner;
import org.lucacanella.csvdiviner.Core.DataDefinitionDiviner.DataDefinitionDivinerInterface;
import org.lucacanella.csvdiviner.Core.DataDefinitionDiviner.SQL.SQLDataDefinitionDiviner;
import org.lucacanella.csvdiviner.Core.Log.LoggerLevel;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CLICreateDDLMode
        implements DivinerCLIExecutionModeInterface {

    public static final String VERSION = "0.1";

    @Parameter(description = "input_file")
    private List<String> inputFilePath = new ArrayList<>();

    @Parameter(names = { "-l", "--logger" }, description = "Set logger level (OFF|INFO|WARNING|ERROR).")
    private String loggerState = "ERROR";

    @Parameter(names = { "-h", "--help-usage" }, description = "Print help.")
    private boolean helpUsage = false;

    @Parameter(names = { "-o", "--output" }, description = "If set, writes the result in a file at the given path.")
    private String outputFilePath = null;

    @Parameter(names = { "-v", "--verbose" }, description = "Prints some information.")
    private boolean verboseMode = false;

    @Parameter(names = { "-c", "--charset" }, description = "Set input file encoding (defaults to UTF-8, see https://docs.oracle.com/javase/8/docs/technotes/guides/intl/encoding.doc.html).")
    private String encoding = "UTF-8";

    @Parameter(names = { "--version" }, description = "Prints version information.")
    private boolean versionMode = false;

    @Parameter(names = { "--silent" }, description = "Sets silent mode (prints only logs).")
    private boolean silentMode = false;

    @Parameter(names = { "-t", "--table" }, description = "The SQL Table name for the CREATE TABLE statement")
    private String tableName = null;

    public void execute(String[] cliArgs) {
        System.out.println(String.join("; ",cliArgs));
        JCommander command = JCommander.newBuilder()
            .addObject(this)
            .build();
        command.parse(cliArgs);

        if(this.versionMode) {
            System.out.format("CsvDiviner, CreateDDL version: %s%CreateDDL Cli Version: %s%s",
                    CsvDiviner.VERSION, System.lineSeparator(), CLIAnalyzerMode.VERSION, System.lineSeparator());
        } else {
            String inputFilePath = this.inputFilePath.get(0);
            if(!this.silentMode) {
                System.out.format("Input file: %s%s", inputFilePath, System.lineSeparator());
            }
            DataDefinitionDivinerConfig config = new DataDefinitionDivinerConfig();
            config.setEncoding(this.encoding);
            config.setLoggerLevel(LoggerLevel.valueOf(this.loggerState));
            config.setContainerName(this.tableName);

            DataDefinitionDivinerInterface ddDiviner = new SQLDataDefinitionDiviner(config);
            if(this.verboseMode) {
                System.out.format("%s settings: %s%s", ddDiviner.getName(), ddDiviner.getConfig(), System.lineSeparator());
            }
            ddDiviner.evaluateFile(inputFilePath);
            String dataDefinitionString = ddDiviner.getDataDefinition();
            if(this.outputFilePath != null) {
                BufferedWriter writer = null;
                try {
                    writer = new BufferedWriter(new FileWriter(this.outputFilePath));
                    writer.write(dataDefinitionString);
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
                System.out.println(dataDefinitionString);
            }
        }

        if(this.helpUsage) {
            command.usage();
            return;
        }
    }
}
