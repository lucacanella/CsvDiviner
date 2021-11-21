package org.lucacanella.csvdiviner.Core.SQLFieldTypeSuggestors;

import org.lucacanella.csvdiviner.Core.CsvDiviner.FieldAnalysis;

/**
 * This suggestor returns only varchar type definition as suitable for each analyzed field.
 * This can ben used to create a table made only of varchar typed columns.
 */
public class PGSQLVarcharOnlyTypeSuggestor extends PGSQLTypeSuggestor {

    private static final String VARCHAR_TYPE_PATTERN = "VARCHAR(%d)%s";

    @Override
    public String getSuggestedDataTypeDefinition(FieldAnalysis f) {
        return getVarcharTypeDefinition(f);
    }
}
