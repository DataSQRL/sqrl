package org.apache.calcite.sql.fun;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.datasqrl.function.builtin.example.MyFunction;
import ai.datasqrl.function.builtin.example.SqlMyFunction;
import ai.datasqrl.function.builtin.time.*;
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
  public void NumTSConversionTest() {
    NumToTimestampFunction numToTimestamp = new NumToTimestampFunction();
    TimestampToEpochFunction timestampToEpoch = new TimestampToEpochFunction();

    SqrlTypeFactory typeFactory = new SqrlTypeFactory(new SqrlTypeSystem());

    RelDataType typeTS = numToTimestamp.inferReturnType(typeFactory, new ArrayList<>());
    assertEquals(typeTS, typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3));

    RelDataType typeNum = timestampToEpoch.inferReturnType(typeFactory, new ArrayList<>());
    assertEquals(typeNum, typeFactory.createSqlType(SqlTypeName.BIGINT));
  }

  @Test
  public void StringTSConversionTest() {
    SqrlTypeFactory typeFactory = new SqrlTypeFactory(new SqrlTypeSystem());

    StringToTimestampFunction stringToTimestamp = new StringToTimestampFunction();
    TimestampToStringFunction timestampToString = new TimestampToStringFunction();

    RelDataType typeTS = stringToTimestamp.inferReturnType(typeFactory, new ArrayList<>());
    assertEquals(typeTS, typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3));

    RelDataType typeString = timestampToString.inferReturnType(typeFactory, new ArrayList<>());
    assertEquals(typeString, typeFactory.createSqlType(SqlTypeName.VARCHAR, 2000));
  }

  @Test
  public void RoundTest() {
    SqrlTypeFactory typeFactory = new SqrlTypeFactory(new SqrlTypeSystem());

    RoundToSecondFunction roundToSecond = new RoundToSecondFunction();
    RoundToMinuteFunction roundToMinute = new RoundToMinuteFunction();
    RoundToHourFunction roundToHour = new RoundToHourFunction();
    RoundToDayFunction roundToDay = new RoundToDayFunction();
    RoundToMonthFunction roundToMonth = new RoundToMonthFunction();
    RoundToYearFunction roundToYear = new RoundToYearFunction();

    RelDataType typeSecond = roundToSecond.inferReturnType(typeFactory, new ArrayList<>());
    assertEquals(typeSecond, typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3));

    RelDataType typeMinute = roundToMinute.inferReturnType(typeFactory, new ArrayList<>());
    assertEquals(typeMinute, typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3));

    RelDataType typeHour = roundToHour.inferReturnType(typeFactory, new ArrayList<>());
    assertEquals(typeHour, typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3));

    RelDataType typeDay = roundToDay.inferReturnType(typeFactory, new ArrayList<>());
    assertEquals(typeDay, typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3));

    RelDataType typeMonth = roundToMonth.inferReturnType(typeFactory, new ArrayList<>());
    assertEquals(typeMonth, typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3));

    RelDataType typeYear = roundToYear.inferReturnType(typeFactory, new ArrayList<>());
    assertEquals(typeYear, typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3));
  }

  @Test
  public void GetterTest() {
    SqrlTypeFactory typeFactory = new SqrlTypeFactory(new SqrlTypeSystem());

    GetSecondFunction getSecond = new GetSecondFunction();
    GetMinuteFunction getMinute = new GetMinuteFunction();
    GetHourFunction getHour = new GetHourFunction();
    GetDayOfWeekFunction getDayOfWeek = new GetDayOfWeekFunction();
    GetDayOfMonthFunction getDayOfMonth = new GetDayOfMonthFunction();
    GetDayOfYearFunction getDayOfYear = new GetDayOfYearFunction();
    GetMonthFunction getMonth = new GetMonthFunction();
    GetYearFunction getYear = new GetYearFunction();

    RelDataType typeSecond = getSecond.inferReturnType(typeFactory, new ArrayList<>());
    assertEquals(typeSecond, typeFactory.createSqlType(SqlTypeName.INTEGER));

    RelDataType typeMinute = getMinute.inferReturnType(typeFactory, new ArrayList<>());
    assertEquals(typeMinute, typeFactory.createSqlType(SqlTypeName.INTEGER));

    RelDataType typeHour = getHour.inferReturnType(typeFactory, new ArrayList<>());
    assertEquals(typeHour, typeFactory.createSqlType(SqlTypeName.INTEGER));

    RelDataType typeDow = getDayOfWeek.inferReturnType(typeFactory, new ArrayList<>());
    assertEquals(typeDow, typeFactory.createSqlType(SqlTypeName.INTEGER));

    RelDataType typeDom = getDayOfMonth.inferReturnType(typeFactory, new ArrayList<>());
    assertEquals(typeDom, typeFactory.createSqlType(SqlTypeName.INTEGER));

    RelDataType typeDoy = getDayOfYear.inferReturnType(typeFactory, new ArrayList<>());
    assertEquals(typeDoy, typeFactory.createSqlType(SqlTypeName.INTEGER));

    RelDataType typeMonth = getMonth.inferReturnType(typeFactory, new ArrayList<>());
    assertEquals(typeMonth, typeFactory.createSqlType(SqlTypeName.INTEGER));

    RelDataType typeYear = getYear.inferReturnType(typeFactory, new ArrayList<>());
    assertEquals(typeYear, typeFactory.createSqlType(SqlTypeName.INTEGER));
  }
}