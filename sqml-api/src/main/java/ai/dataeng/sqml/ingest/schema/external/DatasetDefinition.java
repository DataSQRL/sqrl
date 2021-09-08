package ai.dataeng.sqml.ingest.schema.external;

import ai.dataeng.sqml.schema2.name.NamePath;

import java.util.List;

public class DatasetDefinition {

    public String name;
    public String version;
    public String description;
    public Long applies_at;

    public List<TableDefinition> tables;


}
