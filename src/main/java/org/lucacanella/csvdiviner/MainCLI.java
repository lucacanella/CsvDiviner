package org.lucacanella.csvdiviner;

public class MainCLI {

    public static final String VERSION = "1.2";

    private static final String ANALYZE_MODE = "analyze";
    private static final String CREATE_SQL_DATA_DEFINITION_MODE = "gen_sql";

    private static String[] removeModeArg(String[] args) {
        String[] oldArgs = args;
        args = new String[oldArgs.length - 1];
        System.arraycopy(oldArgs, 1, args, 0, args.length);
        return args;
    }

    public static void main(String[] args) {
        if(args.length < 1) {
            System.out.printf("Selezionare la modalità di esecuzione: '%s', '%s'", ANALYZE_MODE, CREATE_SQL_DATA_DEFINITION_MODE);
            System.out.println();
        }

        DivinerCLIExecutionModeInterface exec;
        String currentMode = args[0];
        switch (currentMode) {
            case CREATE_SQL_DATA_DEFINITION_MODE:
                args = MainCLI.removeModeArg(args);
                exec = new CLICreateDDLMode();
                break;
            case ANALYZE_MODE:
                args = MainCLI.removeModeArg(args);
            default:
                System.out.printf("Attivata modalità esecuzione di default: '%s'", ANALYZE_MODE);
                System.out.println();
                exec = new CLIAnalyzerMode();
                break;
        }
        exec.execute(args);
    }

}
