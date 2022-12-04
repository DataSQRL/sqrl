/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
//package org.apache.calcite.sql.fun;
//
//import com.datasqrl.plan.calcite.PlannerFactory;
//import lombok.Value;
//import org.apache.calcite.rel.type.RelDataType;
//import org.apache.calcite.rel.type.RelDataTypeFactory;
//import org.apache.calcite.sql.SqlFunction;
//import org.apache.calcite.sql.type.SqlTypeName;
//import org.junit.jupiter.api.DynamicTest;
//import org.junit.jupiter.api.TestFactory;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import static com.datasqrl.plan.calcite.SqrlOperatorTable.*;
//import static org.junit.jupiter.api.Assertions.assertEquals;
//
//class TimeLibraryTest {
//  static final RelDataTypeFactory typeFactory = PlannerFactory.getTypeFactory();
//
//  static final RelDataType TSNonNullable = typeFactory.createTypeWithNullability(
//      typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3), false);
//
//  static final RelDataType TSNullable = typeFactory.createTypeWithNullability(
//      TSNonNullable, true);
//
//  final RelDataType StringNonNullable = typeFactory.createTypeWithNullability(
//      typeFactory.createSqlType(SqlTypeName.VARCHAR, 2000), false);
//
//  final RelDataType StringNullable = typeFactory.createTypeWithNullability(
//      StringNonNullable, true);
//
//  static final RelDataType BigIntNonNullable = typeFactory.createTypeWithNullability(
//      typeFactory.createSqlType(SqlTypeName.BIGINT), false);
//
//  static final RelDataType BigIntNullable = typeFactory.createTypeWithNullability(
//      BigIntNonNullable, true);
//
//  @Value
//  class FunctionToTest {
//    SqlFunction function;
//    List<RelDataType> args;
//    RelDataType expected;
//  }
//
//  List<FunctionToTest> tests = List.of(
//    new FunctionToTest(NOW, List.of(), TSNonNullable),
//    new FunctionToTest(EPOCH_TO_TIMESTAMP, List.of(BigIntNonNullable), TSNonNullable),
//    new FunctionToTest(EPOCH_TO_TIMESTAMP, List.of(BigIntNullable), TSNullable),
//    new FunctionToTest(TIMESTAMP_TO_EPOCH, List.of(TSNonNullable), BigIntNonNullable),
//    new FunctionToTest(TIMESTAMP_TO_EPOCH, List.of(TSNullable), BigIntNullable),
//    new FunctionToTest(TIMESTAMP_TO_STRING, List.of(TSNonNullable), StringNonNullable),
//    new FunctionToTest(TIMESTAMP_TO_STRING, List.of(TSNullable), StringNullable),
//    new FunctionToTest(STRING_TO_TIMESTAMP, List.of(TSNonNullable), TSNonNullable),
//    new FunctionToTest(STRING_TO_TIMESTAMP, List.of(TSNullable), TSNullable),
////    new FunctionToTest(TO_UTC, List.of(TSNonNullable, StringNonNullable), TSNonNullable),
////    new FunctionToTest(TO_UTC, List.of(TSNullable, StringNonNullable), TSNullable),
//    new FunctionToTest(AT_ZONE, List.of(TSNonNullable, StringNonNullable), TSNonNullable),
//    new FunctionToTest(AT_ZONE, List.of(TSNullable, StringNonNullable), TSNullable),
//    new FunctionToTest(ROUND_TO_SECOND, List.of(TSNonNullable), TSNonNullable),
//    new FunctionToTest(ROUND_TO_SECOND, List.of(TSNullable), TSNullable),
//    new FunctionToTest(ROUND_TO_MINUTE, List.of(TSNonNullable), TSNonNullable),
//    new FunctionToTest(ROUND_TO_MINUTE, List.of(TSNullable), TSNullable),
//    new FunctionToTest(ROUND_TO_HOUR, List.of(TSNonNullable), TSNonNullable),
//    new FunctionToTest(ROUND_TO_HOUR, List.of(TSNullable), TSNullable),
//    new FunctionToTest(ROUND_TO_DAY, List.of(TSNonNullable), TSNonNullable),
//    new FunctionToTest(ROUND_TO_DAY, List.of(TSNullable), TSNullable),
//    new FunctionToTest(ROUND_TO_MONTH, List.of(TSNonNullable), TSNonNullable),
//    new FunctionToTest(ROUND_TO_MONTH, List.of(TSNullable), TSNullable),
//    new FunctionToTest(ROUND_TO_YEAR, List.of(TSNonNullable), TSNonNullable),
//    new FunctionToTest(ROUND_TO_YEAR, List.of(TSNullable), TSNullable),
//    new FunctionToTest(GET_SECOND, List.of(TSNonNullable), BigIntNonNullable),
//    new FunctionToTest(GET_SECOND, List.of(TSNullable), BigIntNullable),
//    new FunctionToTest(GET_MINUTE, List.of(TSNonNullable), BigIntNonNullable),
//    new FunctionToTest(GET_MINUTE, List.of(TSNullable), BigIntNullable),
//    new FunctionToTest(GET_HOUR, List.of(TSNonNullable), BigIntNonNullable),
//    new FunctionToTest(GET_HOUR, List.of(TSNullable), BigIntNullable),
//    new FunctionToTest(GET_DAY_OF_WEEK, List.of(TSNonNullable), BigIntNonNullable),
//    new FunctionToTest(GET_DAY_OF_WEEK, List.of(TSNullable), BigIntNullable),
//    new FunctionToTest(GET_DAY_OF_MONTH, List.of(TSNonNullable), BigIntNonNullable),
//    new FunctionToTest(GET_DAY_OF_MONTH, List.of(TSNullable), BigIntNullable),
//    new FunctionToTest(GET_DAY_OF_YEAR, List.of(TSNonNullable), BigIntNonNullable),
//    new FunctionToTest(GET_DAY_OF_YEAR, List.of(TSNullable), BigIntNullable),
//    new FunctionToTest(GET_MONTH, List.of(TSNonNullable), BigIntNonNullable),
//    new FunctionToTest(GET_MONTH, List.of(TSNullable), BigIntNullable),
//    new FunctionToTest(GET_YEAR, List.of(TSNonNullable), BigIntNonNullable),
//    new FunctionToTest(GET_YEAR, List.of(TSNullable), BigIntNullable)
//  );
//
//  //extra TO_UTC / at zone test for null second arg
//  @TestFactory
//  Iterable<DynamicTest> testExpectedFunctions() {
//    List<DynamicTest> gen = new ArrayList<>();
//    for (FunctionToTest f : tests) {
//      gen.add(DynamicTest.dynamicTest(String.format(
//          "Fnc %s Args %s Expected %s Nullable %s Test",
//              f.getFunction().getClass().getSimpleName(),
//              f.args, f.expected.getSqlTypeName(), f.expected.isNullable()),
//          () -> {
//            RelDataType type = f.function.inferReturnType(typeFactory, f.args);
//            assertEquals(f.expected+":"+f.expected.isNullable(), type+":"+type.isNullable(), String.format(
//                "Found %s nullable %s",
//                type.getSqlTypeName(), type.isNullable()));
//          }));
//    }
//
//    return gen;
//  }
//}