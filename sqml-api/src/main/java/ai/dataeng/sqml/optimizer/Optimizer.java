package ai.dataeng.sqml.optimizer;

import ai.dataeng.sqml.metadata.Metadata;

public abstract class Optimizer {

  public abstract OptimizerResult optimize(String name, Metadata metadata);

  public abstract static class Builder {
    public abstract Optimizer build();
  }
}
