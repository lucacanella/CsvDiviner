package org.lucacanella.csvdiviner.Core.DataDefinitionDiviner.SQL;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.lucacanella.csvdiviner.Core.Config.DataDefinitionDivinerConfig;
import org.lucacanella.csvdiviner.Core.CsvDiviner.FieldAnalysis;
import org.lucacanella.csvdiviner.Core.DataDefinitionDiviner.DataDefinitionDivinerInterface;
import org.lucacanella.csvdiviner.Core.DataDefinitionDiviner.FieldTypeSuggestors.FieldTypeSuggestor;
import org.lucacanella.csvdiviner.Core.DataDefinitionDiviner.FieldTypeSuggestors.PGSQLTypeSuggestor;
import org.lucacanella.csvdiviner.Core.Log.DivinerLoggerInterface;
import org.lucacanella.csvdiviner.Core.Log.LoggerLevel;
import org.lucacanella.csvdiviner.Core.Log.SynchronizedLogger;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Type;

public class SQLDataDefinitionDiviner
        implements DataDefinitionDivinerInterface {

    DivinerLoggerInterface log;

    DataDefinitionDivinerConfig config;

    String[] ddStrings;

    FieldTypeSuggestor typeSuggestor;

    private final String EOL = System.lineSeparator();

    public String getName() {
        return "SQL Data definition Diviner";
    }

    public SQLDataDefinitionDiviner(DataDefinitionDivinerConfig config) {
        this(config, new SynchronizedLogger(config.getLoggerLevel()));
    }

    public SQLDataDefinitionDiviner(DataDefinitionDivinerConfig config, DivinerLoggerInterface logger) {
        this.config = config;
        this.log = logger;
        this.typeSuggestor = new PGSQLTypeSuggestor();
    }

    public DataDefinitionDivinerConfig getConfig() {
        return this.config;
    }

    public void evaluate(Reader reader) {
        Gson gson = new GsonBuilder().create();
        Type fieldsType = new TypeToken<FieldAnalysis[]>() {}.getType();
        FieldAnalysis[] fields = gson.fromJson(reader, fieldsType);
        ddStrings = new String[fields.length];
        for(int i = 0; i < fields.length; ++i) {
            ddStrings[i] = typeSuggestor.getDataDefinitionString(
                    fields[i]
            );
        }
    }

    public String getDataDefinition() {
        StringBuilder ddString = new StringBuilder();
        if(null == ddStrings || ddStrings.length < 1) {
            throw new IllegalStateException("Errore durante il recupero della definizione dati: Valutazione non valida.");
        } else {
            String tableName = config.getContainerName();
            if(null == tableName) {
                tableName = "table_name";
            }
            return new StringBuilder()
                    .append("CREATE TABLE ")
                    .append(tableName)
                    .append(" (")
                    .append(EOL)
                    .append("\t")
                    .append(String.join(","+EOL+"\t", ddStrings))
                    .append(EOL)
                    .append(");")
                    .toString();
        }
    }

}
