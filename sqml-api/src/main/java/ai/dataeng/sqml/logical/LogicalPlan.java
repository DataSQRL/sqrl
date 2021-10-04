package ai.dataeng.sqml.logical;

import ai.dataeng.sqml.tree.QualifiedName;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;

@Getter
public class LogicalPlan {
  Map<QualifiedName, RelationDefinition> tableDefinitions = new HashMap<>();
  public <R, C> R accept(LogicalPlanVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }

  public void setCurrentDefinition(QualifiedName name, RelationDefinition relationDefinition) {
    tableDefinitions.put(name, relationDefinition);
  }

  public Optional<RelationDefinition> getCurrentDefinition(QualifiedName name) {
    return Optional.ofNullable(tableDefinitions.get(name));
  }

  public List<RelationIdentifier> getBaseEntities() {
    return tableDefinitions.values().stream()
        .filter(e->e.getRelationIdentifier().getName().getParts().size() == 1)
        .map(e->e.getRelationIdentifier())
        .collect(Collectors.toList());
  }
}
