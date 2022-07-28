package ai.datasqrl.parse.tree;

import ai.datasqrl.parse.tree.name.NamePath;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.Getter;

@Getter
public class CreateSubscription extends Node {

  private final SubscriptionType subscriptionType;
  private final NamePath name;
  private final String query;

  public CreateSubscription(Optional<NodeLocation> location, SubscriptionType subscriptionType,
      NamePath name, String query) {
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
