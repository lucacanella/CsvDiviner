package org.lucacanella.csvdiviner.Core.DataDefinitionDiviner;

import org.lucacanella.csvdiviner.Core.Config.DataDefinitionDivinerConfig;

import java.io.Reader;

public interface DataDefinitionDivinerInterface {

    DataDefinitionDivinerConfig getConfig();

    void evaluate(Reader input);

    String getDataDefinition();

    String getName();

}
