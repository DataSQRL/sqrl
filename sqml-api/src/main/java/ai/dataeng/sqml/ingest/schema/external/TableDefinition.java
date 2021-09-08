package ai.dataeng.sqml.ingest.schema.external;

import ai.dataeng.sqml.schema2.name.NamePath;

import java.util.List;

public class TableDefinition extends AbstractElementDefinition {

    public static boolean PARTIAL_SCHEMA_DEFAULT = true;

    public Boolean partial_schema;

    public List<FieldDefinition> columns;
    public List<String> tests;

    public <R, C> R accept(SchemaDefinitionVisitor<R, C> visitor, C context, NamePath location) {
        return visitor.visitTable(this, context, location);
    }


}
