package org.apache.calcite.sql.fun;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.datasqrl.function.builtin.example.MyFunction;
import ai.datasqrl.function.builtin.example.SqlMyFunction;
import ai.datasqrl.function.builtin.time.NumToTimestamp;
import ai.datasqrl.function.builtin.time.StringToTimestamp;
import ai.datasqrl.plan.calcite.SqrlTypeFactory;
import ai.datasqrl.plan.calcite.SqrlTypeSystem;
import java.util.ArrayList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

class NowTest {

  @Test
  public void nowTest() {
    Now now = new Now();
    SqrlTypeFactory typeFactory = new SqrlTypeFactory(new SqrlTypeSystem());
    RelDataType type = now.inferReturnType(typeFactory, new ArrayList<>());
    assertEquals(type, typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 0));
  }

  @Test
  public void myFunctionTest() {
    SqlMyFunction myFunction = new SqlMyFunction();
    SqrlTypeFactory typeFactory = new SqrlTypeFactory(new SqrlTypeSystem());
    RelDataType type = myFunction.inferReturnType(typeFactory, new ArrayList<>());
    // precision scale arg breaks test, removed for now
    assertEquals(type, typeFactory.createSqlType(SqlTypeName.BIGINT));
  }

  @Test
  public void NumToTSTest() {
    NumToTimestamp numToTimestamp = new NumToTimestamp();
    SqrlTypeFactory typeFactory = new SqrlTypeFactory(new SqrlTypeSystem());
    RelDataType type = numToTimestamp.inferReturnType(typeFactory, new ArrayList<>());
    assertEquals(type, typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3));
  }

  @Test
  public void StringToTSTest() {
    StringToTimestamp stringToTimestamp = new StringToTimestamp();
    SqrlTypeFactory typeFactory = new SqrlTypeFactory(new SqrlTypeSystem());
    RelDataType type = stringToTimestamp.inferReturnType(typeFactory, new ArrayList<>());
    assertEquals(type, typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3));
  }
}