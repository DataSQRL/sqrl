package ai.dataeng.sqml.source;

import lombok.Value;

@Value
public class SourceTableQualifiedName {

    private final String dataset;
    private final String table;


    @Override
    public String toString() {
        return dataset + "." + table;
    }
}
