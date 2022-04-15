package ai.datasqrl.graphql.execution.criteria;

import lombok.Value;

@Value
public class EqualsCriteria implements Criteria {
  String envVar;
  String columnName;

  @Override
  public <R, C> R accept(CriteriaVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }
}
