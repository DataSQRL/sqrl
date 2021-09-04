package ai.dataeng.sqml.ingest.schema.external;

import ai.dataeng.sqml.schema2.name.NamePath;

import java.util.List;

public class SchemaDefinition {

    public List<DatasetDefinition> datasets;

    public <R, C> R accept(SchemaDefinitionVisitor<R, C> visitor, C context, NamePath location) {
        return visitor.visitSchema(this, context, location);
    }


}
