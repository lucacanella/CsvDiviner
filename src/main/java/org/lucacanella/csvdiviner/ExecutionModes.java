package org.lucacanella.csvdiviner;

public enum ExecutionModes {
    ANALYZE_MODE("analyze"),
    CREATE_SQL_DATA_DEFINITION_MODE("gen_sql")
    ;

    private final String modeText;

    ExecutionModes(final String text) {
        this.modeText = text;
    }

    @Override
    public String toString() {
        return modeText;
    }

    public static ExecutionModes fromString(String text) {
        for (ExecutionModes b : ExecutionModes.values()) {
            if (b.modeText.equalsIgnoreCase(text)) {
                return b;
            }
        }
        return null;
    }

}
