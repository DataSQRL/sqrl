package ai.dataeng.sqml.logical;

import ai.dataeng.sqml.tree.QualifiedName;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;

@Getter
public class LogicalPlan {

  private final Collection<RelationDefinition> tableDefinitions;
  private final List<SubscriptionDefinition> subscriptionDefinitions;

  public LogicalPlan(Collection<RelationDefinition> tableDefinitions,
      List<SubscriptionDefinition> subscriptionDefinitions) {
    this.tableDefinitions = tableDefinitions;
    this.subscriptionDefinitions = subscriptionDefinitions;
  }

  public List<RelationIdentifier> getBaseEntities() {
    return tableDefinitions.stream()
        .filter(e->e.getRelationIdentifier().getName().getParts().size() == 1)
        .map(e->e.getRelationIdentifier())
        .collect(Collectors.toList());
  }

  public static class Builder {
    Map<QualifiedName, RelationDefinition> tableDefinitions = new HashMap<>();
    List<SubscriptionDefinition> subscriptionDefinitions = new ArrayList<>();
    public void setCurrentDefinition(QualifiedName name, RelationDefinition relationDefinition) {
      tableDefinitions.put(name, relationDefinition);
    }

    public Optional<RelationDefinition> getCurrentDefinition(QualifiedName name) {
      return Optional.ofNullable(tableDefinitions.get(name));
    }

    public void setSubscriptionDefinition(SubscriptionDefinition subscriptionDefinition) {
      subscriptionDefinitions.add(subscriptionDefinition);

    }
    public LogicalPlan build() {
      return new LogicalPlan(tableDefinitions.values(), subscriptionDefinitions);
    }

  }
}
