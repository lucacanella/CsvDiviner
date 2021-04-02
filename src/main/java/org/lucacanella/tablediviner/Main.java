package org.lucacanella.tablediviner;

import com.beust.jcommander.Parameters;
import org.lucacanella.tablediviner.Core.TableDiviner;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;

@Parameters(resourceBundle = "MainMessages")
public class Main {

    public static final String VERSION = "0.1b";

    @Parameter(names = { "-l", "--logger" }, descriptionKey = "CliParamLoggerLevel")
    private String loggerState = "WARN";

    @Parameter(names = { "-h", "--help-usage" }, descriptionKey="CliParamPrintUsage")
    private boolean helpUsage = false;

    @Parameter(names = { "-w", "--wizard" }, descriptionKey="CliParamWizardMode")
    private boolean configureMode = false;

    @Parameter(names = { "-c", "--config" }, descriptionKey="CliParamConfigFile")
    private String configfile = null;

    @Parameter(names = { "-v", "--version" }, descriptionKey="CliParamPrintVersion")
    private boolean versionMode = false;

    @Parameter(names = { "--silent" }, descriptionKey="CliParamSilentMode")
    private boolean silentMode = false;

    private static final String DEFAULT_LOCALE = "it";
    private static final String DEFAULT_COUNTRY = "IT";

    protected  static void out(String msg) {
        out(msg, true);
    }

    protected  static void out(String msg, boolean nl) {
        if(nl) {
            System.out.println(msg);
        } else {
            System.out.print(msg);
        }
    }

    protected static void outFormat(String output, String ...params) {
        System.out.format(output, params);
    }

    protected static void outFormatNL(String output, String ...params) {
        System.out.format(output, params);
        System.out.println();
    }

    public static void main(String[] args) {
        Locale currentLocale;
        ResourceBundle messages;

        currentLocale = new Locale(Main.DEFAULT_LOCALE, Main.DEFAULT_COUNTRY);
        messages =
                ResourceBundle.getBundle("MainMessages",currentLocale);

        var main = new Main();
        JCommander command = JCommander.newBuilder()
                .addObject(main)
                .build();
        command.parse(args);

        if(main.versionMode) {
            outFormat(messages.getString("VersionMessageTmpl"),
                    TableDiviner.VERSION, System.lineSeparator(), Main.VERSION, System.lineSeparator());
        }

        if(!main.silentMode && null != main.configfile) {
            outFormatNL(messages.getString("ConfigFileInfoTmpl"), main.configfile);
        }

        if(main.helpUsage || (!main.versionMode && !main.configureMode && null == main.configfile)) {
            command.usage();
            return;
        }
    }

}
