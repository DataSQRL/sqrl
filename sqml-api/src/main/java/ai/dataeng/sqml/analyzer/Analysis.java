package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.db.keyvalue.HierarchyKeyValueStore;
import ai.dataeng.sqml.logical3.LogicalPlan;
import ai.dataeng.sqml.logical3.LogicalPlan.Builder;
import ai.dataeng.sqml.physical.PhysicalModel;
import ai.dataeng.sqml.tree.Assignment;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QueryAssignment;
import ai.dataeng.sqml.tree.Script;
import ai.dataeng.sqml.tree.Table;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class Analysis {
  private final Map<Node, Scope> scopes = new LinkedHashMap<>();
  private PhysicalModel physicalModel;
  private final Builder builder;
  private Map<Node, StatementAnalysis> statementAnalysisMap = new HashMap<>();

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

  public void setStatementAnalysis(Node node, StatementAnalysis analysis) {
    statementAnalysisMap.put(node, analysis);
  }
  public StatementAnalysis getStatementAnalysis(Node node) {
    return statementAnalysisMap.get(node);
  }
}
