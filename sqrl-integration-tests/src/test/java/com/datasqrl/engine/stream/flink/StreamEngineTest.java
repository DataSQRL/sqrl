//package com.datasqrl.engine.stream.flink;
//
//import com.datasqrl.plan.calcite.OptimizationStage;
//import com.datasqrl.plan.calcite.Planner;
//import com.datasqrl.plan.calcite.PlannerFactory;
//import org.apache.calcite.jdbc.SqrlCalciteSchema;
//import org.apache.calcite.rel.RelNode;
//import org.apache.calcite.rel.core.JoinRelType;
//import org.apache.calcite.schema.Table;
//import org.apache.calcite.tools.RelBuilder;
//import org.junit.jupiter.api.Test;
//
//class StreamEngineTest {
//
//  @Test
//  public void test() {
//    //1. Create a logical plan, scan 2 tables from different sources and join
//    //2. Define the table: KafkaDataConnector
//    //3. define a rule to convert it to either: flink or calcite
//    //4. Execute ?
//
//    // Create cluster
//
//    SqrlCalciteSchema schema = new SqrlCalciteSchema();
//    schema.add("table1", (Table)null);
//    schema.add("table2", (Table)null);
//
//    Planner planner = new PlannerFactory(schema.plus()).createPlanner();
//
//    RelBuilder relBuilder = planner.getRelBuilder();
//    RelNode n = relBuilder
//        .scan("table1")
//        .scan("table2")
//        .join(JoinRelType.FULL, relBuilder.getRexBuilder().makeLiteral(true))
//        .build();
//
//    RelNode enumerable = planner.transform(OptimizationStage.CALCITE_ENGINE, n);
//
//    System.out.println(enumerable.explain());
//
//  }
//}