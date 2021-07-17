package ai.dataeng.sqml.optimizer;

import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.tree.Script;

public abstract class OptimizerResult {

  private final Script script;
  private final Analysis analysis;

  public OptimizerResult(Script script, Analysis analysis) {
    this.script = script;
    this.analysis = analysis;
  }

  public Script getScript() {
    return script;
  }

  public Analysis getAnalysis() {
    return analysis;
  }
}
