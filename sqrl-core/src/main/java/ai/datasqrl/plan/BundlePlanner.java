package ai.datasqrl.plan;

import ai.datasqrl.config.BundleOptions;
import ai.datasqrl.config.EnvironmentConfiguration;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.provider.DatabaseConnectionProvider;
import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.config.scripts.ScriptBundle;
import ai.datasqrl.config.scripts.SqrlScript;
import ai.datasqrl.parse.ConfiguredSqrlParser;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.physical.PhysicalPlan;
import ai.datasqrl.physical.PhysicalPlanner;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.calcite.PlannerFactory;
import ai.datasqrl.plan.calcite.table.AbstractRelationalTable;
import ai.datasqrl.plan.calcite.table.CalciteTableFactory;
import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import ai.datasqrl.plan.global.DAGPlanner;
import ai.datasqrl.plan.global.OptimizedDAG;
import ai.datasqrl.plan.local.generate.Resolve;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.plan.local.generate.Session;
import ai.datasqrl.plan.queries.APIQuery;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.ScriptNode;

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
    Planner planner = (new PlannerFactory(CalciteSchema.createRootSchema(false, false).plus())).createPlanner();

    Env env = planMain(bundle.getMainScript(), planner);

    PhysicalPlanner physicalPlanner = new PhysicalPlanner(options.getImportManager(),
        options.getDbConnection(),
        options.getStreamEngine(), planner);

    DAGPlanner dagPlanner = new DAGPlanner(planner);

    //todo: Add api queries
    List<APIQuery> queries = new ArrayList<APIQuery>();
    List.of("Orders").stream().map(t -> getLatestTable(env.getRelSchema(),t,VirtualRelationalTable.class))
        .map(t -> t.get()).forEach(vt -> {
          String tblName =  vt.getNameId();
          RelNode rel = planner.getRelBuilder().scan(tblName).build();
          queries.add(new APIQuery(tblName.substring(0,tblName.indexOf(Name.NAME_DELIMITER)), rel));
        });

    OptimizedDAG dag = dagPlanner.plan(env.getRelSchema(), queries);

    return physicalPlanner.plan(dag);
  }


  public static<T extends AbstractRelationalTable> Optional<T> getLatestTable(CalciteSchema relSchema, String tableName, Class<T> tableClass) {
    String normalizedName = Name.system(tableName).getCanonical();
    //Table names have an appended uuid - find the right tablename first. We assume tables are in the order in which they were created
    return relSchema.getTableNames().stream().filter(s -> s.substring(0,s.indexOf(Name.NAME_DELIMITER)).equals(normalizedName))
        .filter(s -> tableClass.isInstance(relSchema.getTable(s,true).getTable()))
        //Get most recently added table
        .sorted((a,b) -> -Integer.compare(CalciteTableFactory.getTableOrdinal(a),CalciteTableFactory.getTableOrdinal(b)))
        .findFirst().map(s -> tableClass.cast(relSchema.getTable(s,true).getTable()));
  }

  private Env planMain(SqrlScript mainScript, Planner planner) {
    ConfiguredSqrlParser parser = new ConfiguredSqrlParser(errorCollector);
    ScriptNode scriptNode = parser.parse(mainScript.getContent());
    Resolve resolve = new Resolve(null);

    Session session = new Session(errorCollector, options.getImportManager(), planner);

    return resolve.planDag(session, scriptNode);
  }
}
