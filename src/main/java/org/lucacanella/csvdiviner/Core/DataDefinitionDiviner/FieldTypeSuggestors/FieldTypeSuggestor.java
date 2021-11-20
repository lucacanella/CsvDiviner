package org.lucacanella.csvdiviner.Core.DataDefinitionDiviner.FieldTypeSuggestors;

import org.lucacanella.csvdiviner.Core.CsvDiviner.FieldAnalysis;

public abstract class FieldTypeSuggestor {

    public abstract String getDataDefinitionString(FieldAnalysis f);

    protected SuggestedType detectSuggestedType(FieldAnalysis f) {
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
            return SuggestedType.INTEGER;
        } else if (containsDateTime) {
            return SuggestedType.INTEGER;
        } else if (containsDate){
            return SuggestedType.DATE;
        } else if (containsMixedTypes) {
            return SuggestedType.MIXED;
        } else {
            return SuggestedType.ND;
        }
    }

}
