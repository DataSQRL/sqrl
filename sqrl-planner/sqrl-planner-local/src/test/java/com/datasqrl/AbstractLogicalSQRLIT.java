/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.graphql.APIConnectorLookup;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.plan.MainScript;
import com.datasqrl.plan.global.DAGPlanner;
import com.datasqrl.plan.global.SqrlDAG;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.plan.table.AbstractRelationalTable;
import com.datasqrl.plan.table.ScriptRelationalTable;
import com.datasqrl.plan.validate.ScriptPlanner;
import com.datasqrl.util.CalciteUtil;
import com.google.common.base.Preconditions;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.AfterEach;

public class AbstractLogicalSQRLIT extends AbstractEngineIT {

  @AfterEach
  public void tearDown() {
    super.tearDown();
    errors = null;
  }

  protected void plan(String query) {
    ScriptPlanner planner = injector.getInstance(ScriptPlanner.class);
    ModuleLoader moduleLoader = injector.getInstance(ModuleLoader.class);
    try {
      planner.plan(new StringMainScript(query), moduleLoader);
    } catch (CollectedException e) {
      System.out.println(ErrorPrinter.prettyPrint(errors));
      throw e;
    }
  }

  @AllArgsConstructor
  @Getter
  public class StringMainScript implements MainScript {
    final Optional<Path> path = Optional.empty();
    String content;
  }


  protected SqrlDAG planDAG(String script, Collection<String> queryTables) {
    plan(script);
    DAGPlanner dagPlanner = injector.getInstance(DAGPlanner.class);

    createAPIQueries(queryTables);
    return dagPlanner.planLogical();
  }

  /**
   * Adds a table scan for every table in the collection
   *
   * @param queryTables
   * @return
   */
  protected void createAPIQueries(Collection<String> queryTables) {
    APIConnectorManager apiManager = injector.getInstance(APIConnectorManager.class);
    SqrlSchema sqrlSchema = framework.getSchema();
    for (String tableName : queryTables) {
      Optional<ScriptRelationalTable> vtOpt = getLatestTable(sqrlSchema, tableName,
              ScriptRelationalTable.class);
      Preconditions.checkArgument(vtOpt.isPresent(), "No such table: %s", tableName);
      ScriptRelationalTable vt = vtOpt.get();
      RelBuilder relBuilder = framework.getQueryPlanner().getRelBuilder()
              .scan(vt.getNameId());
      relBuilder = CalciteUtil.projectOutNested(relBuilder);
      apiManager.addQuery(new APIQuery(tableName + "-query", NamePath.ROOT, relBuilder.build(),
          List.of(), List.of(), false));
    }
  }


  public static <T extends AbstractRelationalTable> Optional<T> getLatestTable(
          SqrlSchema sqrlSchema, String tableName, Class<T> tableClass) {
    String normalizedName = Name.system(tableName).getCanonical();
    //Table names have an appended uuid - find the right tablename first. We assume tables are in the order in which they were created
    return sqrlSchema.getTableNames().stream().filter(s -> s.indexOf(Name.NAME_DELIMITER) != -1)
            .filter(s -> s.substring(0, s.indexOf(Name.NAME_DELIMITER)).equals(normalizedName))
            .filter(s ->
                    tableClass.isInstance(sqrlSchema.getTable(s, false).getTable()))
            //Get most recently added table
            .sorted((a, b) -> -Integer.compare(getTableOrdinal(a),
                    getTableOrdinal(b)))
            .findFirst().map(s -> tableClass.cast(sqrlSchema.getTable(s, false).getTable()))
            ;
  }

  public static int getTableOrdinal(String tableId) {
    int idx = tableId.lastIndexOf(Name.NAME_DELIMITER);
    return Integer.parseInt(tableId.substring(idx + 1));
  }


}
