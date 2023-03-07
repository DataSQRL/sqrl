package org.apache.flink.table.api.internal;

import java.io.IOException;
import java.util.Collections;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.catalog.FunctionCatalogOperatorTable;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.resource.ResourceType;
import org.apache.flink.table.resource.ResourceUri;

/**
 * Lives in the flink package to access the protected createTable function
 */
public class FlinkEnvProxy {

  public static Table relNodeQuery(RelNode relNode, TableEnvironmentImpl environment) {
    PlannerQueryOperation plannerQueryOperation = new PlannerQueryOperation(relNode);
    plannerQueryOperation.getResolvedSchema();
    return environment.createTable(plannerQueryOperation);
  }

  //
  public static FunctionCatalog getFunctionCatalog(TableEnvironmentImpl environment) {
    return environment.functionCatalog;
  }

  public static SqlOperatorTable getOperatorTable(TableEnvironmentImpl env) {
    FunctionCatalog catalog = env.functionCatalog;

    SqlOperatorTable operatorTable = SqlOperatorTables.chain(
        new FunctionCatalogOperatorTable(
            catalog,
            env.getCatalogManager().getDataTypeFactory(),
            new FlinkTypeFactory(env.getClass().getClassLoader(), FlinkTypeSystem.INSTANCE),
            null),
        FlinkSqlOperatorTable.instance()
    );
    return operatorTable;
  }

  public static void addJar(StatementSet statementSet, String jarPath) {
    StatementSetImpl impl = (StatementSetImpl) statementSet;
    TableEnvironmentImpl tEnv = (TableEnvironmentImpl) impl.tableEnvironment;
    ResourceUri resourceUri = new ResourceUri(ResourceType.JAR, jarPath);
    try {
      tEnv.resourceManager.registerJarResources(Collections.singletonList(resourceUri));
    } catch (IOException e) {
      throw new TableException(
          String.format("Could not register the specified resource [%s].", resourceUri),
          e);
    }
  }
}
