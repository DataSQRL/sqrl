package ai.dataeng.sqml.ingest.schema.external;

import ai.dataeng.sqml.schema2.name.NamePath;

import java.util.List;
import java.util.Map;

public class FieldDefinition extends AbstractElementDefinition implements FieldTypeDefinition {

    public String type;
    public List<FieldDefinition> columns;
    public List<String> tests;

    public Map<String, FieldTypeDefinitionImpl> mixed;

    public boolean isMixed() {
        return mixed!=null;
    }

    public <R, C> R accept(SchemaDefinitionVisitor<R, C> visitor, C context, NamePath location) {
        return visitor.visitField(this, context, location);
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public List<FieldDefinition> getColumns() {
        return columns;
    }

    @Override
    public List<String> getTests() {
        return tests;
    }
}
