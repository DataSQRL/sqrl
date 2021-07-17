package ai.dataeng.sqml;

import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.dag.Dag;
import ai.dataeng.sqml.tree.DefaultTraversalVisitor;
import ai.dataeng.sqml.tree.Script;
import graphql.schema.GraphQLSchema;

public class GraphqlSchemaBuilder {

  public static Builder newGraphqlSchema() {
    return new Builder();
  }

  public static class Builder {

    private Dag dag;

    public Builder dag(Dag dag) {
      this.dag = dag;
      return this;
    }

    public GraphQLSchema build(){
      Script script = dag.getOptimizationResult().getScript();
      Analysis analysis = dag.getOptimizationResult().getAnalysis();
      Visitor visitor = new Visitor(analysis);
      script.accept(visitor, null);


      return null;
    }
  }

  static class Visitor extends DefaultTraversalVisitor<Object, Void> {

    private final Analysis analysis;

    public Visitor(Analysis analysis) {
      this.analysis = analysis;
    }

  }
}
