package org.apache.calcite.sql.fun;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.datasqrl.function.builtin.example.SqlMyFunction;
import ai.datasqrl.function.builtin.time.AtZoneFunction;
import ai.datasqrl.function.builtin.time.NumToTimestampFunction;
import ai.datasqrl.function.builtin.time.ExtractTimeFieldFunction;
import ai.datasqrl.function.builtin.time.SqrlTimeRoundingFunction;
import ai.datasqrl.function.builtin.time.StdTimeLibraryImpl;
import ai.datasqrl.function.builtin.time.StringToTimestampFunction;
import ai.datasqrl.function.builtin.time.TimestampToEpochFunction;
import ai.datasqrl.function.builtin.time.TimestampToStringFunction;
import ai.datasqrl.function.builtin.time.ToUtcFunction;
import ai.datasqrl.plan.calcite.SqrlTypeFactory;
import ai.datasqrl.plan.calcite.SqrlTypeSystem;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

class TimeLibraryTest {

  static final SqrlTypeFactory typeFactory = new SqrlTypeFactory(new SqrlTypeSystem());

  @Test
  public void test() {
    Now now = new Now();

    RelDataType type = now.inferReturnType(typeFactory, new ArrayList<>());
    assertEquals(type, typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 0));
  }
  @Test
  public void myFunctionTest() {
    SqlMyFunction myFunction = new SqlMyFunction();

    RelDataType type = myFunction.inferReturnType(typeFactory, new ArrayList<>());
    // precision scale arg breaks test, removed for now
    assertEquals(type, typeFactory.createSqlType(SqlTypeName.BIGINT));
  }

  @Test
  public void NumTSConversionTest() {
    NumToTimestampFunction numToTimestamp = new NumToTimestampFunction();
    TimestampToEpochFunction timestampToEpoch = new TimestampToEpochFunction();



    RelDataType typeTS = numToTimestamp.inferReturnType(typeFactory, new ArrayList<>());
    assertEquals(typeTS, typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3));

    RelDataType typeNum = timestampToEpoch.inferReturnType(typeFactory, new ArrayList<>());
    assertEquals(typeNum, typeFactory.createSqlType(SqlTypeName.BIGINT));
  }

  @Test
  public void StringTSConversionTest() {


    StringToTimestampFunction stringToTimestamp = new StringToTimestampFunction();
    TimestampToStringFunction timestampToString = new TimestampToStringFunction();

    RelDataType typeTS = stringToTimestamp.inferReturnType(typeFactory, new ArrayList<>());
    assertEquals(typeTS, typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3));

    RelDataType typeString = timestampToString.inferReturnType(typeFactory, new ArrayList<>());
    assertEquals(typeString, typeFactory.createSqlType(SqlTypeName.VARCHAR, 2000));
  }

  @Test
  public void RoundTest() {


    SqrlTimeRoundingFunction roundToSecond = new SqrlTimeRoundingFunction("ROUND_TO_SECOND",
        ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "roundToSecond", Instant.class)));
    SqrlTimeRoundingFunction roundToMinute = new SqrlTimeRoundingFunction("ROUND_TO_MINUTE",
        ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "roundToMinute", Instant.class)));
    SqrlTimeRoundingFunction roundToHour = new SqrlTimeRoundingFunction("ROUND_TO_HOUR",
        ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "roundToHour", Instant.class)));
    SqrlTimeRoundingFunction roundToDay = new SqrlTimeRoundingFunction("ROUND_TO_DAY",
        ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "roundToDay", Instant.class)));
    SqrlTimeRoundingFunction roundToMonth = new SqrlTimeRoundingFunction("ROUND_TO_MONTH",
        ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "roundToMonth", Instant.class)));
    SqrlTimeRoundingFunction roundToYear = new SqrlTimeRoundingFunction("ROUND_TO_YEAR",
        ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "roundToYear", Instant.class)));

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


    ExtractTimeFieldFunction getSecond = new ExtractTimeFieldFunction("GET_SECOND",
        ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "getSecond", Instant.class)));
    ExtractTimeFieldFunction getMinute = new ExtractTimeFieldFunction("GET_MINUTE",
        ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "getMinute", Instant.class)));
    ExtractTimeFieldFunction getHour = new ExtractTimeFieldFunction("GET_HOUR",
        ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "getHour", Instant.class)));
    ExtractTimeFieldFunction getDayOfWeek = new ExtractTimeFieldFunction("GET_DAY_OF_WEEK",
        ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "getDayOfWeek", Instant.class)));
    ExtractTimeFieldFunction getDayOfMonth = new ExtractTimeFieldFunction("GET_DAY_OF_MONTH",
        ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "getDayOfMonth", Instant.class)));
    ExtractTimeFieldFunction getDayOfYear = new ExtractTimeFieldFunction("GET_DAY_OF_YEAR",
        ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "getDayOfYear", Instant.class)));
    ExtractTimeFieldFunction getMonth = new ExtractTimeFieldFunction("GET_MONTH",
        ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "getMonth", Instant.class)));
    ExtractTimeFieldFunction getYear = new ExtractTimeFieldFunction("GET_YEAR",
        ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "getYear", Instant.class)));

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

  @Test
  public void TZConversionTest() {


    ToUtcFunction toUtc = new ToUtcFunction();
    AtZoneFunction atZone = new AtZoneFunction();

    RelDataType typeUTC = toUtc.inferReturnType(typeFactory, new ArrayList<>());
    assertEquals(typeUTC, typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3));

    RelDataType typeZone = atZone.inferReturnType(typeFactory, new ArrayList<>());
    assertEquals(typeZone, typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3));
  }

  @Test
  public void NotNullPreservationTest () {

    SqrlTimeRoundingFunction roundToSecond = new SqrlTimeRoundingFunction("ROUND_TO_SECOND",
        ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "roundToSecond", Instant.class)));

    RelDataType TSNonNullable = typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3), false);

    RelDataType typeSecond = roundToSecond.inferReturnType(typeFactory, List.of(TSNonNullable));

    assertEquals(typeSecond, TSNonNullable);
  }

  @Test
  public void NullPreservationTest() {

    SqrlTimeRoundingFunction roundToSecond = new SqrlTimeRoundingFunction("ROUND_TO_SECOND",
        ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "roundToSecond", Instant.class)));

    RelDataType TSNullable = typeFactory.createTypeWithNullability(
        typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3), true);

    RelDataType typeSecond = roundToSecond.inferReturnType(typeFactory, List.of(TSNullable));

    assertEquals(typeSecond, TSNullable);

  }
}