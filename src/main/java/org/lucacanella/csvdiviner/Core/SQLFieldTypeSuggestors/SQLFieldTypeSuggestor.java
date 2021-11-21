package org.lucacanella.csvdiviner.Core.SQLFieldTypeSuggestors;

import org.lucacanella.csvdiviner.Core.CsvDiviner.FieldAnalysis;

public abstract class SQLFieldTypeSuggestor {

    public enum SuggestedType {
        VARCHAR,
        INTEGER,
        BIGINT,
        NUMERIC,
        DATETIME,
        DATE,
        MIXED,
        ND;
    };

    /**
     * Returns the suggested data type definition string for this field, i.e. VARCHAR(255), INTEGER, NUMERIC, ...
     * This string can be used in a CREATE TABLE statement to create a database column that is appropriate to store
     * data from an analyzed field.
     * @param f the result of a field analysis
     * @return the data type string to be used in an SQL CREATE TABLE statement
     */
    public abstract String getSuggestedDataTypeDefinition(FieldAnalysis f);
}
