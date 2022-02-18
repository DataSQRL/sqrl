package ai.dataeng.sqml.type.schema.external;

import java.util.List;

public class TableDefinition extends AbstractElementDefinition {

    public static boolean PARTIAL_SCHEMA_DEFAULT = true;

    public Boolean partial_schema;

    public List<FieldDefinition> columns;
    public List<String> tests;

}
