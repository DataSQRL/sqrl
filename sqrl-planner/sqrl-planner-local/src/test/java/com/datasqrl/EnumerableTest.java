//package com.datasqrl;
//
//import com.datasqrl.TestSqrlFramework.TestQueryPlanner;
//import com.datasqrl.calcite.Dialect;
//import com.datasqrl.canonicalizer.NamePath;
//import com.datasqrl.module.SqrlModule;
//import com.datasqrl.plan.local.analyze.MockModuleLoader;
//import com.datasqrl.util.DataContextImpl;
//import java.math.BigInteger;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Optional;
//import lombok.SneakyThrows;
//import org.apache.calcite.adapter.enumerable.EnumerableRel;
//import org.apache.calcite.linq4j.Enumerable;
//import org.apache.calcite.linq4j.Enumerator;
//import org.apache.calcite.rel.RelNode;
//import org.apache.calcite.runtime.ArrayBindable;
//import org.junit.jupiter.api.Disabled;
//import org.junit.jupiter.api.Test;
//
//class EnumerableTest {
//
//  @SneakyThrows
//  @Test
//  @Disabled //Disabled after moving to flink delegating sql functions
//  public void test() throws ClassNotFoundException {
//    TestSqrlFramework framework = new TestSqrlFramework(null);
//    Map<NamePath, SqrlModule> retail = TestModuleFactory.createRetail(framework);
//    MockModuleLoader moduleLoader = new MockModuleLoader(null,
//        retail,
//        Optional.empty());
//    TestQueryPlanner planner = framework.getQueryPlanner(moduleLoader);
//    planner.planSqrl("IMPORT ecommerce-data.Product");
//    planner.planSqrl("IMPORT secure.*");
//
//    RelNode plan = planner.plan(Dialect.CALCITE,
//        planner.parse(Dialect.CALCITE, "SELECT RandomID(productid) AS secureid FROM TABLE(Product())"));
//
//    RelNode relNode = planner.expandMacros(plan);
//
//    EnumerableRel enumerableRel = framework.getQueryPlanner().convertToEnumerableRel(relNode);
//    ClassLoader classLoader = framework.getQueryPlanner()
//        .compile("ToJsonExec", enumerableRel, new HashMap<>());
//    Class<?> toJsonExec = Class.forName("ToJsonExec", true, classLoader);
//    ArrayBindable bindable = (ArrayBindable) toJsonExec.getConstructors()[0].newInstance();
//
//    Enumerable<Object[]> bind = bindable.bind(new DataContextImpl(framework,
//        ()-> List.<Object[]>of(new Object[]{"col1", "col2", 20})));
//    Enumerator<Object[]> enumerator = bind.enumerator();
//
//    while (enumerator.moveNext()) {
//      System.out.println(enumerator.current());
//    }
//  }
//}
