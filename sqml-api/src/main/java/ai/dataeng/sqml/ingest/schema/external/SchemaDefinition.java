package ai.dataeng.sqml.ingest.schema.external;

import ai.dataeng.sqml.schema2.name.NamePath;

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
