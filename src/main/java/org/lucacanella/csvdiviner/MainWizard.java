package org.lucacanella.csvdiviner;

import com.beust.jcommander.Parameters;
import org.lucacanella.csvdiviner.Core.CsvDiviner.CsvDiviner;


import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.lucacanella.csvdiviner.Core.Config.CsvDivinerConfig;

import java.io.File;
import java.util.*;
import java.util.regex.Pattern;

@Parameters(resourceBundle = "MainMessages")
public class MainWizard {

    public static final String VERSION = "0.1b";

    @Parameter(names = { "-l", "--logger" }, descriptionKey = "CliParamLoggerLevel")
    private String loggerState = "WARNING";

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

    protected static final Scanner in = new Scanner(System.in);

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

    protected static String inQuestionOptional(String question) {
        out(question, false);
        String inputLine = in.nextLine();
        if(null == inputLine || inputLine.length() < 1) {
            inputLine = null;
        }
        return inputLine;
    }

    protected static String in(String defaultValue, ResourceBundle messages) {
        String inputLine = in.nextLine();
        while (inputLine == null || inputLine.length() < 1) {
            if(defaultValue != null) {
                inputLine = defaultValue;
            } else {
                out(messages.getString("CliWizInvalidAnswer"));
                inputLine = in.nextLine();
            }
        }
        return inputLine;
    }

    protected static String inQuestion(String question, ResourceBundle messages, String defaultValue) {
        out(question, false);
        return in(defaultValue, messages);
    }

    static final Set<String> suppoortedParsers = new HashSet<>() {{
        add("CSV");
        add("TSV");
    }};

    public static void main(String[] args) {
        Locale currentLocale = new Locale(MainWizard.DEFAULT_LOCALE, MainWizard.DEFAULT_COUNTRY);
        ResourceBundle messages =
                ResourceBundle.getBundle("MainMessages",currentLocale);

        var main = new MainWizard();
        JCommander command = JCommander.newBuilder()
                .addObject(main)
                .build();
        command.parse(args);

        if(main.versionMode) {
            outFormat(messages.getString("VersionMessageTmpl"),
                    CsvDiviner.VERSION, System.lineSeparator(), MainWizard.VERSION, System.lineSeparator());
        } else if(!main.silentMode && null != main.configfile) {
            outFormatNL(messages.getString("ConfigFileInfoTmpl"), main.configfile);
        } else if(main.helpUsage || (!main.configureMode && null == main.configfile)) {
            command.usage();
        } else {
            startWizard(messages, currentLocale);
        }
    }

    private static void startWizard(ResourceBundle messages, Locale locale) {
        String filetype = inQuestion(messages.getString("CliWizFileTypeQuestion"), messages, "CSV");
        if(!suppoortedParsers.contains(filetype)) {
            outFormatNL(messages.getString("InvalidFileTypeSpecifiedTmpl"), filetype);
            System.exit(-1);
        }
        String filepath = inQuestion(messages.getString("CliWizFilePathQuestion"), messages, null);
        File inputFile = new File(filepath);
        if(!inputFile.exists()) {
            outFormatNL(messages.getString("FileDoesntExistsTmpl"), filetype);
            System.exit(-1);
        }
        switch (filetype) {
            case "CSV":
                continueForCsv(inputFile, locale, messages);
                break;
            case "TSV":
                continueForTsv(inputFile, locale, messages);
                break;
            default:
                outFormatNL(messages.getString("InvalidFileTypeSpecifiedTmpl"), filetype);
                System.exit(-1);
                break;
        }
    }

    private static void continueForTsv(File inputFile, Locale locale, ResourceBundle messages) {
        throw new UnsupportedOperationException(messages.getString("NotSupportedYet"));
        /*TsvParserSettings parserSettings = new TsvParserSettings();

        // separatore record (terminatore linee)
        String lineSep = inQuestion(messages.getString("CliWizLineSeparatorQuestion"), messages, "true");
        switch(lineSep.toLowerCase(locale)) {
            case "crlf":
                parserSettings.getFormat().setLineSeparator("\r\n");
                break;
            case "lf":
                parserSettings.getFormat().setLineSeparator("\n");
                break;
            case "auto":
                parserSettings.setLineSeparatorDetectionEnabled(true);
                break;
            default:
                outFormatNL(messages.getString("CliWizInvalidValueSpecifiedTmpl"), "CRLF, LF, AUTO");
                System.exit(-1);
                break;
        }

        String nullValue = inQuestionOptional(messages.getString("CliWizNullValueQuestion"));
        parserSettings.setNullValue(nullValue);*/
    }

    private static void continueForCsv(File inputFile, Locale locale, ResourceBundle messages) {
        CsvDivinerConfig divinerConfig = new CsvDivinerConfig();

        // separatore record (terminatore linee)
        String lineSep = inQuestion(messages.getString("CliWizLineSeparatorQuestion"), messages, "true");
        switch(lineSep.toLowerCase(locale)) {
            case "lf":
                divinerConfig.setSeparator('\n');
                break;
            case "auto":
                divinerConfig.getParserSettings().setLineSeparatorDetectionEnabled(true);
                break;
            case "":
                outFormatNL(messages.getString("CliWizInvalidValueSpecifiedTmpl"), "LF, AUTO, <single_char>");
                System.exit(-1);
                break;
            default:
                if(lineSep.length() > 1) {
                    outFormatNL(messages.getString("CliWizInvalidValueSpecifiedTmpl"), "LF, AUTO, <single_char>");
                    System.exit(-1);
                    break;
                }
                divinerConfig.setSeparator(lineSep.charAt(0));
        }

        String nullValue = inQuestionOptional(messages.getString("CliWizNullValueQuestion"));
        if(null != nullValue) {
            divinerConfig.getParserSettings().setNullValue(nullValue);
        }

        String emptyValue = inQuestionOptional(messages.getString("CliWizEmptyValueQuestion"));
        if(null != nullValue) {
            divinerConfig.getParserSettings().setEmptyValue(emptyValue);
        }

        String specifyHeaders = inQuestion(messages.getString("CliWizSetHeadersQuestion"), messages, "NO");
        if(Pattern.matches(messages.getString("CliWizYesAnswerPatt"), specifyHeaders)) {
            divinerConfig.getParserSettings().setHeaders("a", "b", "c", "d", "e");
        }

        // sets the maximum number of characters to read in each column.
        // The default is 4096 characters. You need this to avoid OutOfMemoryErrors in case a file
        // does not have a valid format. In such cases the parser might just keep reading from the input
        // until its end or the memory is exhausted. This sets a limit which avoids unwanted JVM crashes.
        divinerConfig.getParserSettings().setMaxCharsPerColumn(4096);
    }

}
