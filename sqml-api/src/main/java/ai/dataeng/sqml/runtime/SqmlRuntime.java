package ai.dataeng.sqml.runtime;

import ai.dataeng.sqml.dag.Dag;
import ai.dataeng.sqml.execution.ExecutionResult;
import ai.dataeng.sqml.execution.ExecutionStrategy;
import ai.dataeng.sqml.query.Query;
import ai.dataeng.sqml.vertex.Vertex;
import java.util.List;
import java.util.concurrent.Future;

public class SqmlRuntime {

  private final Dag dag;
  private final ExecutionStrategy executionStrategy;

  public SqmlRuntime(Dag dag, ExecutionStrategy executionStrategy) {
    this.dag = dag;
    this.executionStrategy = executionStrategy;
  }

  public static Builder builder() {
    return new Builder();
  }

  public ExecutionResult execute(String queryRef) {
    Query query = dag.getQueries().getQuery(queryRef);
    return executionStrategy.execute(query);
  }

  public void start() {
    executionStrategy.init(dag.getVertices());
  }

  public static class Builder {

    private ExecutionStrategy executionStrategy;
    private Dag dag;

    public Builder dag(Dag dag) {
      this.dag = dag;
      return this;
    }

    public Builder executionStrategy(ExecutionStrategy executionStrategy) {
      this.executionStrategy = executionStrategy;
      return this;
    }

    public SqmlRuntime build() {
      return new SqmlRuntime(dag, executionStrategy);
    }
  }
}
