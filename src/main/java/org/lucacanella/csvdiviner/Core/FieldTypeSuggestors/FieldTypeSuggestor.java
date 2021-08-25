package org.lucacanella.csvdiviner.Core.FieldTypeSuggestors;

import org.lucacanella.csvdiviner.Core.FieldAnalysis;

public abstract class FieldTypeSuggestor {

    public enum SuggestedType {
        VARCHAR,
        INTEGER,
        NUMERIC,
        DATETIME,
        DATE,
        MIXED,
        ND;
    };

    public abstract String getSuggestedType(FieldAnalysis f);
}
