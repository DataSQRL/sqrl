/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.config.PipelineFactory;
import com.datasqrl.flink.FlinkConverter;
import com.datasqrl.plan.hints.SqrlHintStrategyTable;
import com.datasqrl.plan.rules.SqrlRelMetadataProvider;
import com.datasqrl.util.DatabaseHandle;
import com.google.inject.Injector;
import java.util.Optional;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.junit.jupiter.api.AfterEach;

public abstract class AbstractEngineIT {

  public DatabaseHandle database = null;
  protected Injector injector;

  public SqrlFramework framework = createFramework();

  public SqrlFramework createFramework() {
    SqrlFramework framework1 = new SqrlFramework(SqrlRelMetadataProvider.INSTANCE,
        SqrlHintStrategyTable.getHintStrategyTable(), NameCanonicalizer.SYSTEM);
    SqlFunction function = new FlinkConverter(framework1.getQueryPlanner().getRexBuilder(), framework1.getTypeFactory())
        .convertFunction("COALESCE", "COALESCE", BuiltInFunctionDefinitions.COALESCE);

    framework1.getSqrlOperatorTable().addFunction("COALESCE", function);

    framework1.getSqrlOperatorTable().addFunction("NOW", FlinkSqlOperatorTable.NOW);

    SqlFunction greatest = new FlinkConverter(framework1.getQueryPlanner().getRexBuilder(), framework1.getTypeFactory())
        .convertFunction("GREATEST", "GREATEST", BuiltInFunctionDefinitions.GREATEST);
    framework1.getSqrlOperatorTable().addFunction("GREATEST", greatest);


    return framework1;
  }

  @AfterEach
  public void tearDown() {
    if (database != null) {
      database.cleanUp();
      database = null;
    }
  }

  protected PipelineFactory initialize(IntegrationTestSettings settings, Injector injector) {
    this.injector = injector;
    return this.initialize(settings);
  }

  protected PipelineFactory initialize(IntegrationTestSettings settings) {
    if (database == null) {
      Pair<DatabaseHandle, PipelineFactory> setup = settings.getSqrlSettings();
      database = setup.getLeft();
      return setup.getRight();
    } else {
      return null;
    }
  }
}
