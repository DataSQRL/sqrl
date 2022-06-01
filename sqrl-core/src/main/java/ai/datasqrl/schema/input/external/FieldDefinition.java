package ai.datasqrl.schema.input.external;

import java.util.List;
import java.util.Map;

public class FieldDefinition extends AbstractElementDefinition implements FieldTypeDefinition {

  public String type;
  public List<FieldDefinition> columns;
  public List<String> tests;

  public Map<String, FieldTypeDefinitionImpl> mixed;

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
