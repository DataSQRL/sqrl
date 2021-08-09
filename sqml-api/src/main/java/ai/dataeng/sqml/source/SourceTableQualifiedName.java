package ai.dataeng.sqml.source;

import lombok.NonNull;
import lombok.Value;

@Value
public class SourceTableQualifiedName {

    @NonNull
    private final String dataset;
    private final String table;


    @Override
    public String toString() {
        return dataset + "." + table;
    }
}
