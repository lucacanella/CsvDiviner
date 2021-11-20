package org.lucacanella.csvdiviner.Core.DataDefinitionDiviner;

import org.lucacanella.csvdiviner.Core.Config.DataDefinitionDivinerConfig;

public interface DataDefinitionDivinerInterface {

    public DataDefinitionDivinerConfig getConfig();

    public void evaluateFile(String csvDivinerResultFilePath);

    public String getDataDefinition();

    public String getName();

}
