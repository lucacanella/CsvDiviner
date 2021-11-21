package org.lucacanella.csvdiviner.Core.Config;

public class DataDefinitionDivinerConfig
        extends AbstractDivinerConfig {

    String encoding;

    String containerName;

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public void setContainerName(String containerName) {
        this.containerName = containerName;
    }

    public String getContainerName() {
        return containerName;
    }

}
