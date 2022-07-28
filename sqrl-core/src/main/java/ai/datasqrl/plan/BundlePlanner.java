package ai.datasqrl.plan;

import ai.datasqrl.config.BundleOptions;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.config.scripts.SqrlScript;
import ai.datasqrl.parse.ConfiguredSqrlParser;
import ai.datasqrl.parse.tree.ScriptNode;
import ai.datasqrl.physical.PhysicalPlan;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.plan.calcite.SqrlTypeFactory;
import ai.datasqrl.plan.calcite.SqrlTypeSystem;
import ai.datasqrl.plan.calcite.sqrl.table.CalciteTableFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;

/**
 * Creates a logical and physical plan for a SQRL {@link ScriptBundle} submitted to the DataSQRL server for
 * compilation or execution.
 *
 * @see ScriptBundle
 */
@Slf4j
public class BundlePlanner {

  private final BundleOptions options;
  private final ErrorCollector errorCollector = ErrorCollector.root();

  public BundlePlanner(BundleOptions options) {
    this.options = options;
  }

  public PhysicalPlan processBundle(ScriptBundle bundle) {
    planMain(bundle.getMainScript());
    return null;
  }

  private void planMain(SqrlScript mainScript) {
  }
}
