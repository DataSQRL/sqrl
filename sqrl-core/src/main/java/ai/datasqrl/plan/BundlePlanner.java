package ai.datasqrl.plan;

import ai.datasqrl.config.BundleOptions;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.config.scripts.SqrlScript;
import ai.datasqrl.physical.PhysicalPlan;
import lombok.extern.slf4j.Slf4j;

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
//    Resolve resolve = GeneratorBuilder.build(options.getImportManager(), errorCollector);
//    ConfiguredSqrlParser parser = new ConfiguredSqrlParser(errorCollector);
//    ScriptNode scriptNode = parser.parse(mainScript.getContent());
//    generator.generate(scriptNode);
    // List<QueryRelationalTable> tables = generator.getTableMapper().getTableMap().values().stream().filter(QueryRelationalTable.class::isInstance)
    //         .map(QueryRelationalTable.class::cast).collect(Collectors.toList());

    //


  }
}
