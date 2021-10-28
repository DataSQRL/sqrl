package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.logical3.LogicalPlan;
import ai.dataeng.sqml.logical3.LogicalPlan.Builder;
import ai.dataeng.sqml.physical.PhysicalModel;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.Script;
import java.util.LinkedHashMap;
import java.util.Map;

public class Analysis {
  private final Map<Node, Scope> scopes = new LinkedHashMap<>();
  private PhysicalModel physicalModel;
  private final Builder builder;

  public Analysis(Script script, PhysicalModel physicalModel,
      Builder builder) {
    this.physicalModel = physicalModel;
    this.builder = builder;
  }

  public void setScope(Node node, Scope scope) {
    scopes.put(node, scope);
  }

  public Scope getScope(Node node) {
    return scopes.get(node);
  }

  public Builder getPlanBuilder() {
    return builder;
  }

  public LogicalPlan getPlan() {
    return builder.build();
  }

  public PhysicalModel getPhysicalModel() {
    return physicalModel;
  }
}
