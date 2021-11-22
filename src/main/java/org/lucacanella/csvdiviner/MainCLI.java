package org.lucacanella.csvdiviner;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class MainCLI {

    public static final String VERSION = "1.2";

    private static String[] removeModeArg(String[] args) {
        String[] oldArgs = args;
        args = new String[oldArgs.length - 1];
        System.arraycopy(oldArgs, 1, args, 0, args.length);
        return args;
    }

    public static void main(String[] args) {
        if(args.length < 1) {
            System.out.printf("Selezionare la modalità di esecuzione: '%s', '%s'",
                    ExecutionModes.ANALYZE_MODE,
                    ExecutionModes.CREATE_SQL_DATA_DEFINITION_MODE
            );
            System.out.println();
            return;
        }

        Set<String> availableModesText =
                Arrays.stream(ExecutionModes.values()).map((em) -> em.toString())
                        .collect(Collectors.toSet());


        List<Integer> argsSegments = new LinkedList<Integer>();
        for(int i = 0; i < args.length; ++i) {
            if (availableModesText.contains(args[i])) {
                argsSegments.add(i);
            }
        }

        if(argsSegments.size() < 1) {
            new CLIAnalyzerMode().execute(args);
        } else {
            argsSegments.add(args.length);
            String lastOutput = null;
            for(int i = 0; i < argsSegments.size() - 1; ++i) {
                int segmentStart = argsSegments.get(i);
                if(segmentStart < args.length) {
                    ExecutionModes executionMode = ExecutionModes.fromString(args[segmentStart]);
                    int segmentEnd = argsSegments.get(i + 1);
                    int segmentSize = segmentEnd - segmentStart;
                    String[] modeArgs = null;
                    if (segmentSize > 1) {
                        modeArgs = new String[segmentSize - 1];
                        System.arraycopy(args, segmentStart + 1, modeArgs, 0, segmentSize - 1);
                    }
                    DivinerCLIExecutionModeInterface exec = null;
                    switch (executionMode) {
                        case CREATE_SQL_DATA_DEFINITION_MODE:
                            exec = new CLICreateDDLMode();
                            break;
                        case ANALYZE_MODE:
                            exec = new CLIAnalyzerMode();
                            break;
                        default:
                            throw new RuntimeException("Execution mode is invalid or not yet supported.");
                    }
                    if(null != lastOutput) {
                        try {
                            exec.pipeIn(lastOutput);
                        } catch (PipeInputNotSupportedException e) {
                            System.out.println();
                        }
                    }
                    lastOutput = exec.execute(modeArgs);
                }
            }
        }
    }

}
