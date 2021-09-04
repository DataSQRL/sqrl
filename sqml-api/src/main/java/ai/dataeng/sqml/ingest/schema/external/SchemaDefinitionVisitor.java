package ai.dataeng.sqml.ingest.schema.external;

import ai.dataeng.sqml.schema2.name.NamePath;

public class SchemaDefinitionVisitor<R, C> {

    public R visitSchema(SchemaDefinition schema, C context, NamePath location) {
        return null;
    }

    public R visitDataset(DatasetDefinition dataset, C context, NamePath location) {
        return null;
    }

    public R visitTable(TableDefinition table, C context, NamePath location) {
        return null;
    }

    public R visitField(FieldDefinition field, C context, NamePath location) {
        return null;
    }

}
