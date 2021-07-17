package ai.dataeng.sqml;

import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.optimizer.OptimizerResult;
import ai.dataeng.sqml.tree.Script;
import java.util.ArrayList;
import java.util.List;

public class PostgresResult extends OptimizerResult {
  public List<MigrationObject> migrationObjects = new ArrayList<>();

  public PostgresResult(Script script, Analysis analysis) {
    super(script, analysis);
  }

  public static class MigrationObject {
    public String getSql() {
      return null;
    }
  }

  public List<MigrationObject> getMigrationObjects() {
    return migrationObjects;
  }
}
