package org.lucacanella.csvdiviner;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.lucacanella.csvdiviner.Core.Config.DataDefinitionDivinerConfig;
import org.lucacanella.csvdiviner.Core.CsvDiviner.CsvDiviner;
import org.lucacanella.csvdiviner.Core.DataDefinitionDiviner.DataDefinitionDivinerInterface;
import org.lucacanella.csvdiviner.Core.DataDefinitionDiviner.SQL.SQLDataDefinitionDiviner;
import org.lucacanella.csvdiviner.Core.Log.LoggerLevel;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class CLICreateDDLMode
        implements DivinerCLIExecutionModeInterface {

    public static final String VERSION = "0.1";

    private final String EOL = System.lineSeparator();

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

    private String pipedInput;

    public String execute(String[] cliArgs) {
        String result = null;
        JCommander command = JCommander.newBuilder()
            .addObject(this)
            .build();
        command.parse(cliArgs);

        if(this.versionMode) {
            System.out.format("CsvDiviner, CreateDDL version: %s%CreateDDL Cli Version: %s%s",
                    CsvDiviner.VERSION, EOL, CLIAnalyzerMode.VERSION, EOL);
        } else {
            DataDefinitionDivinerConfig config = new DataDefinitionDivinerConfig();
            config.setEncoding(this.encoding);
            config.setLoggerLevel(LoggerLevel.valueOf(this.loggerState));
            config.setContainerName(this.tableName);

            DataDefinitionDivinerInterface ddDiviner = new SQLDataDefinitionDiviner(config);
            if(this.verboseMode) {
                System.out.format("%s settings: %s%s", ddDiviner.getName(), ddDiviner.getConfig(), EOL);
            }

            if(null == pipedInput) {
                if (this.inputFilePath.size() < 1) {
                    throw new RuntimeException("Missing input file name parameter.");
                }
                String inputFilePath = this.inputFilePath.get(0);
                if (!this.silentMode) {
                    System.out.format("Input file: %s%s",
                            inputFilePath, EOL
                    );
                }
                try {
                    FileReader reader = new FileReader(inputFilePath);
                    ddDiviner.evaluate(reader);
                } catch (FileNotFoundException e) {
                    System.out.format(
                            "Errore critico durante la lettura del file %s: %s%s",
                            inputFilePath, e.getMessage(), EOL
                    );
                }
            } else {
                ddDiviner.evaluate(new StringReader(pipedInput));
            }
            result = ddDiviner.getDataDefinition();
            if(this.outputFilePath != null) {
                try {
                    BufferedWriter writer = null;
                    writer = new BufferedWriter(new FileWriter(this.outputFilePath));
                    writer.write(result);
                    writer.close();
                    if(!this.silentMode) {
                        System.out.format("\tL'output è stato scritto nel file %s%s", this.outputFilePath, EOL);
                    }
                } catch (IOException e) {
                    System.out.printf(
                            "Errore imprevisto durante la scrittura del file di output:%s\t%s%s",
                            EOL, e.getMessage(), EOL
                    );
                }
            } else {
                System.out.printf(
                        "Risultato dell'analisi: %s%s%s", EOL, result, EOL
                );
            }
        }

        if(this.helpUsage) {
            command.usage();
        }
        return result;
    }

    @Override
    public void pipeIn(String input) throws PipeInputNotSupportedException {
        this.pipedInput = input;
    }
}
