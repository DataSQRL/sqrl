package com.datasqrl;

import static org.junit.jupiter.api.Assertions.*;

import com.datasqrl.TestSqrlFramework.TestQueryPlanner;
import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.plan.local.analyze.MockModuleLoader;
import com.datasqrl.plan.local.analyze.RetailSqrlModule;
import com.datasqrl.util.DataContextImpl;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.Test;

class EnumerableTest {

  @Test
  public void test() {
    TestSqrlFramework framework = new TestSqrlFramework();
    Map<NamePath, SqrlModule> retail = TestModuleFactory.createRetail(framework);
    MockModuleLoader moduleLoader = new MockModuleLoader(null,
        retail,
        Optional.empty());
    TestQueryPlanner planner = framework.getQueryPlanner(moduleLoader);
    planner.planSqrl("IMPORT ecommerce-data.Product");

    RelNode plan = planner.plan(Dialect.CALCITE,
        planner.parse(Dialect.CALCITE, "SELECT * FROM TABLE(Product())"));

    RelNode relNode = planner.expandMacros(plan);

    Enumerator execute = planner.execute(relNode, new DataContextImpl(framework));

    // Should be empty
    while (execute.moveNext()) {
      System.out.println(execute.current());
    }
  }
}