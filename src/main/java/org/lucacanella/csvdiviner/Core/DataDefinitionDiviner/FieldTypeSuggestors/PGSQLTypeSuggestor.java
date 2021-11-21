package org.lucacanella.csvdiviner.Core.DataDefinitionDiviner.FieldTypeSuggestors;

import org.lucacanella.csvdiviner.Core.CsvDiviner.FieldAnalysis;

public class PGSQLTypeSuggestor
        extends FieldTypeSuggestor {

    protected static final String VARCHAR_TYPE_PATTERN = "VARCHAR(%d)%s";
    protected static final String BIGINT_TYPE_PATTERN = "BIGINT%s";
    protected static final String INTEGER_TYPE_PATTERN = "INTEGER%s";
    protected static final String NUMERIC_TYPE_PATTERN = "NUMERIC%s";
    protected static final String DATE_TYPE_PATTERN = "DATE%s";
    protected static final String DATETIME_TYPE_PATTERN = "TIMESTAMP%s";
    protected static final String NOT_NULL_STR = " NOT NULL";
    protected static final String EMPTY_STRING = "";


    public String getDataDefinitionString(FieldAnalysis f) {
        String typeDef;
        switch (detectSuggestedType(f)) {
            case INTEGER:
                typeDef = getIntegerTypeDefinition(f);
                break;
            case DATE:
                typeDef = getDateTypeDefinition(f);
                break;
            case DATETIME:
                typeDef = getDateTimeTypeDefinition(f);
                break;
            case NUMERIC:
                typeDef = getNumericTypeDefinition(f);
                break;
            case VARCHAR:
            case MIXED:
            case ND:
            default:
                typeDef = getVarcharTypeDefinition(f);
                break;
        }
        return String.format("%s %s", f.getHeader(), typeDef);
    }

    private String getVarcharTypeDefinition(FieldAnalysis f) {
        int maxLength = f.getMaxStrLen();
        return String.format(VARCHAR_TYPE_PATTERN,
                maxLength > 0 ? maxLength : 1,
                f.hasNulls() ? EMPTY_STRING : NOT_NULL_STR
        );
    }

    private String getDateTypeDefinition(FieldAnalysis f) {
        return String.format(DATE_TYPE_PATTERN, f.hasNulls() ? EMPTY_STRING : NOT_NULL_STR);
    }

    private String getDateTimeTypeDefinition(FieldAnalysis f) {
        return String.format(DATETIME_TYPE_PATTERN, f.hasNulls() ? EMPTY_STRING : NOT_NULL_STR);
    }

    private String getIntegerTypeDefinition(FieldAnalysis f) {
        if(f.getMaxStrLen() > 10) {
            return String.format(BIGINT_TYPE_PATTERN, f.hasNulls() ? EMPTY_STRING : NOT_NULL_STR);
        } else {
            return String.format(INTEGER_TYPE_PATTERN, f.hasNulls() ? EMPTY_STRING : NOT_NULL_STR);
        }
    }

    private String getNumericTypeDefinition(FieldAnalysis f) {
        return String.format(NUMERIC_TYPE_PATTERN, f.hasNulls() ? EMPTY_STRING : NOT_NULL_STR);
    }

}
