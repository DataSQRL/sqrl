package ai.datasqrl.graphql.execution.criteria;

public interface Criteria {

  <R, C> R accept(CriteriaVisitor<R, C> visitor, C context);
}
