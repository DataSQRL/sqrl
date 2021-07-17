package ai.dataeng.sqml;

import ai.dataeng.sqml.plan.ProjectNode;
import ai.dataeng.sqml.relation.VariableReferenceExpression;
import ai.dataeng.sqml.sql.tree.Relation;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Constructs an entity-like model for sqml (for analysis)
 */
public class StubModel {
  public List<VariableReferenceExpression> arguments;
  public List<ModelRelation> relations = new ArrayList<>();
  public static class ModelRelation {
    public String name;
    public ProjectNode relation;
    public List<ModelRelation> additionalRelations;

    public ModelRelation(String name, ProjectNode relation,
        List<ModelRelation> additionalRelations) {
      this.name = name;
      this.relation = relation;
      this.additionalRelations = additionalRelations;
    }
  }
}