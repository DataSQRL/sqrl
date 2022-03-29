package ai.dataeng.sqml.parser.sqrl.analyzer;

import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.name.Name;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class Scope {
  private Optional<Table> contextTable;
  private Node node;
  private List<Field> fields;

  public Node getNode() {
    return node;
  }

  public void setNode(Node node) {
    this.node = node;
  }

  public List<Field> getFields() {
    return fields;
  }

  public void setFields(List<Field> fields) {
    this.fields = fields;
  }

  public List<Field> resolveFieldsWithPrefix(Optional<Name> starPrefix) {
    return null;
  }
}
