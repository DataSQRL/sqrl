package ai.dataeng.sqml.execution;

import ai.dataeng.sqml.query.Query;
import ai.dataeng.sqml.vertex.Vertex;
import java.util.List;

public abstract class ExecutionStrategy {

  public abstract ExecutionResult execute(Query query);

  public abstract void init(List<Vertex> vertices);
  public abstract void shutdown();

}
