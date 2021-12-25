package ai.dataeng.sqml.execution.flink.ingest.schema.external;

import java.util.Collections;
import java.util.List;

public class SchemaDefinition {

    public String version;
    public List<DatasetDefinition> datasets;


    public static SchemaDefinition empty() {
        SchemaDefinition def = new SchemaDefinition();
        def.datasets = Collections.EMPTY_LIST;
        def.version = SchemaImport.VERSION.getId();
        return def;
    }

}
