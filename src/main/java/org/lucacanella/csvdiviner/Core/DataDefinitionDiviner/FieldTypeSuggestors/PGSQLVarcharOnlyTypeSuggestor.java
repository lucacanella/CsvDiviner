package org.lucacanella.csvdiviner.Core.DataDefinitionDiviner.FieldTypeSuggestors;

import org.lucacanella.csvdiviner.Core.CsvDiviner.FieldAnalysis;

public class PGSQLVarcharOnlyTypeSuggestor extends PGSQLTypeSuggestor {

    private static final String VARCHAR_TYPE_PATTERN = "VARCHAR(%d)%s";

    public SuggestedType getSuggestedType(FieldAnalysis f) {
        return SuggestedType.VARCHAR;
    }
}
