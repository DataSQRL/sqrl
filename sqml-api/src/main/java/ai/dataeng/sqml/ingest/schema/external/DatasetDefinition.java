package ai.dataeng.sqml.ingest.schema.external;

import ai.dataeng.sqml.schema2.name.NamePath;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

public class DatasetDefinition {

    public String name;
    public String version;
    public String description;
    public List<TableDefinition> tables;


}
