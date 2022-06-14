package ai.datasqrl.schema.input.external;

import java.util.List;

public class FieldTypeDefinitionImpl implements FieldTypeDefinition {

  public String type;
  public List<FieldDefinition> columns;
  public List<String> tests;

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
