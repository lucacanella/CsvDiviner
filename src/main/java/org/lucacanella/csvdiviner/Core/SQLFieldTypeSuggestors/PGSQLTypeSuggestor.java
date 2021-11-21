package org.lucacanella.csvdiviner.Core.SQLFieldTypeSuggestors;

import org.lucacanella.csvdiviner.Core.CsvDiviner.FieldAnalysis;

public class PGSQLTypeSuggestor extends SQLFieldTypeSuggestor {

    protected static final String VARCHAR_TYPE_PATTERN = "VARCHAR(%d)%s";
    protected static final String INTEGER_TYPE_PATTERN = "INTEGER%s";
    protected static final String NUMERIC_TYPE_PATTERN = "NUMERIC%s";
    protected static final String BIGINT_TYPE_PATTERN = "NUMERIC%s";
    protected static final String DATETIME_TYPE_PATTERN = "DATE%s";
    protected static final String DATE_TYPE_PATTERN = "DATETIME%s";
    protected static final String NOT_NULL_STR = " NOT NULL";
    protected static final String EMPTY_STRING = "";

    public SuggestedType detectSuggestedType(FieldAnalysis f) {
        if(!f.hasValues()) {
            return SuggestedType.ND;
        }
        boolean containsOtherValues = f.containsOtherValues()
                , containsIntegers = f.containsIntegers()
                , containsFloats = f.containsFloats()
                , containsDate = f.containsDate()
                , containsDateTime = f.containsDateTime()
                ;
        int howManyMixedTypes =
                (containsFloats || containsIntegers ? 1 : 0)
                        + (containsDate ? 1 : 0)
                        + (containsDateTime ? 1 : 0)
                        + (containsOtherValues ? 1 : 0)
                ;
        boolean containsMixedTypes = howManyMixedTypes > 1;

        if(containsOtherValues) {
            return SuggestedType.VARCHAR;
        } else if(containsFloats) {
            return SuggestedType.NUMERIC;
        } else if (containsIntegers) {
            if(f.getMaxStrLen() <= 10) {
                return SuggestedType.INTEGER;
            } else {
                return SuggestedType.BIGINT;
            }
        } else if (containsDateTime) {
            return SuggestedType.DATETIME;
        } else if (containsDate){
            return SuggestedType.DATE;
        } else if (containsMixedTypes) {
            return SuggestedType.MIXED;
        } else {
            return SuggestedType.ND;
        }
    }

    @Override
    public String getSuggestedDataTypeDefinition(FieldAnalysis f) {
        String typeDef;
        switch (detectSuggestedType(f)) {
            case VARCHAR:
                typeDef = getVarcharTypeDefinition(f);
                break;
            case INTEGER:
                typeDef = getIntegerTypeDefinition(f);
                break;
            case BIGINT:
                typeDef = getBigintTypeDefinition(f);
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
            case MIXED:
            case ND:
            default:
                typeDef = getDefaultTypeDefinition(f);
                break;
        }
        return typeDef;
    }

    public String getVarcharTypeDefinition(FieldAnalysis f) {
        int maxLength = f.getMaxStrLen();
        return String.format(VARCHAR_TYPE_PATTERN,
                maxLength > 0 ? maxLength : 1,
                f.hasNulls() ? NOT_NULL_STR : EMPTY_STRING
        );
    }

    /**
     * Fallback type definition (defaults to Varchar)
     * @param f
     * @return
     */
    private String getDefaultTypeDefinition(FieldAnalysis f) {
        return getVarcharTypeDefinition(f);
    }

    /**
     * @todo
     * @param f
     * @return
     */
    private String getBigintTypeDefinition(FieldAnalysis f) {
        return null;
    }

    /**
     * @todo
     * @param f
     * @return
     */
    private String getDateTypeDefinition(FieldAnalysis f) {
        return null;
    }

    /**
     * @todo
     * @param f
     * @return
     */
    private String getDateTimeTypeDefinition(FieldAnalysis f) {
        return null;
    }

    /**
     * @todo
     * @param f
     * @return
     */
    private String getIntegerTypeDefinition(FieldAnalysis f) {
        return null;
    }

    /**
     * @todo
     * @param f
     * @return
     */
    private String getNumericTypeDefinition(FieldAnalysis f) {
        return null;
    }
}
