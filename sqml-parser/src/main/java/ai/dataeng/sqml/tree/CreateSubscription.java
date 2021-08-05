package ai.dataeng.sqml.tree;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class CreateSubscription extends Node {

  private final SubscriptionType subscriptionType;
  private final QualifiedName name;
  private final Query query;

  public CreateSubscription(Optional<NodeLocation> location, SubscriptionType subscriptionType, QualifiedName name, Query query) {
    super(location);
    this.subscriptionType = subscriptionType;
    this.name = name;
    this.query = query;
  }

  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitCreateSubscription(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CreateSubscription that = (CreateSubscription) o;
    return subscriptionType == that.subscriptionType && Objects.equals(name, that.name)
        && Objects.equals(query, that.query);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of(query);
  }

  @Override
  public int hashCode() {
    return Objects.hash(subscriptionType, name, query);
  }

  @Override
  public String toString() {
    return "CreateSubscription{" +
        "subscriptionType=" + subscriptionType +
        ", name=" + name +
        ", query=" + query +
        '}';
  }
}
