package org.lucacanella.csvdiviner;

public interface DivinerCLIExecutionModeInterface {
    String execute(String[] cliArgs);
    void pipeIn(String input) throws PipeInputNotSupportedException;
}
