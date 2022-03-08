package org.apache.calcite.schema;

import ai.dataeng.sqml.Environment;
import ai.dataeng.sqml.ScriptDeployment;
import ai.dataeng.sqml.config.SqrlSettings;
import ai.dataeng.sqml.planner.Planner;
import ai.dataeng.sqml.planner.Script;
import ai.dataeng.sqml.planner.operator.C360Test;
import ai.dataeng.sqml.planner.operator.DefaultTestSettings;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.impl.VertxInternal;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class UnsqrlrTest {

  static Environment env;
  static SqrlSettings sqrlSettings;
  static Planner planner;
  static Script script;

  @SneakyThrows
  @BeforeAll
  public static void before() {
    VertxOptions vertxOptions = new VertxOptions();
    VertxInternal vertx = (VertxInternal) Vertx.vertx(vertxOptions);
    sqrlSettings = DefaultTestSettings.create(vertx);
    env = Environment.create(sqrlSettings);

    env.getDatasetRegistry().addOrUpdateSource(C360Test.dd, new ProcessMessage.ProcessBundle<>());
    planner = sqrlSettings.getHeuristicPlannerProvider().createPlanner();
    script = env.compile(ScriptDeployment.of(C360Test.bundle));
  }

  @Test
  public void test2() {
  }
}