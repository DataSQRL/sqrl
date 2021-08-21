package ai.dataeng.sqml.optimizer;

import ai.dataeng.sqml.PostgresResult;
import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.analyzer.Analyzer;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.rewrite.AddColumnsFromStatistics;
import ai.dataeng.sqml.rewrite.ScriptRewriter;
import ai.dataeng.sqml.tree.Script;
import ai.dataeng.sqml.vertex.SqlVertexFactory;
import java.util.List;

public class ShreddingSqlOptimizer extends Optimizer {

  @Override
  public OptimizerResult optimize(String name, Metadata metadata) {
    Script script = metadata.getScript(name);

    ScriptRewriter scriptRewriter = new ScriptRewriter(
        List.of(
            new AddColumnsFromStatistics(metadata)));
    Script rewritten = scriptRewriter.rewrite(script);

    Analysis analysis = Analyzer.analyze(rewritten, metadata);

    return new PostgresResult(rewritten, analysis);
  }


  public static Builder newOptimizer() {
    return new Builder();
  }

  public static class Builder extends Optimizer.Builder {

    public Optimizer build() {
      return new ShreddingSqlOptimizer();
    }

    public Builder vertexFactory(SqlVertexFactory newSqlVertexFactory) {
      return new Builder();
    }
  }
}
