package ai.dataeng.sqml.execution.flink.ingest.source;

import lombok.NonNull;
import lombok.Value;

@Value
public class SourceTableQualifiedName {

    @NonNull
    private final String dataset;
    @NonNull
    private final String table;


    @Override
    public String toString() {
        return dataset + "." + table;
    }

    public static SourceTableQualifiedName of(String dataset, String table) {
        return new SourceTableQualifiedName(dataset, table);
    }
}
