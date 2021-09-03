package ai.dataeng.sqml.ingest.schema.external;

import ai.dataeng.sqml.ingest.schema.name.NamePath;

import java.util.List;

public class DatasetDefinition {

    public String name;
    public String version;
    public String description;
    public Long applies_after;

    public List<TableDefinition> tables;

    public <R, C> R accept(SchemaDefinitionVisitor<R, C> visitor, C context, NamePath location) {
        return visitor.visitDataset(this, context, location);
    }


}
