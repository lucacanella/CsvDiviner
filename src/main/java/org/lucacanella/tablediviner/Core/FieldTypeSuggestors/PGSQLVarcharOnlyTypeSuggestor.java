package org.lucacanella.tablediviner.Core.FieldTypeSuggestors;

import org.lucacanella.tablediviner.Core.FieldAnalysis;

public class PGSQLVarcharOnlyTypeSuggestor extends PGSQLTypeSuggestor {

    private static final String VARCHAR_TYPE_PATTERN = "VARCHAR(%d)%s";

    @Override
    public String getSuggestedType(FieldAnalysis f) {
        return getVarcharTypeDefinition(f);
    }
}
