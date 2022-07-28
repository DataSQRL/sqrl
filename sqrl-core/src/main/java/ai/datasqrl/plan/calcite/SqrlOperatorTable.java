package ai.datasqrl.plan.calcite;

import ai.datasqrl.function.builtin.example.SqlMyFunction;
import ai.datasqrl.function.builtin.time.AtZoneFunction;
import ai.datasqrl.function.builtin.time.NumToTimestampFunction;
import ai.datasqrl.function.builtin.time.SqrlGettersFunction;
import ai.datasqrl.function.builtin.time.SqrlRoundingFunction;
import ai.datasqrl.function.builtin.time.StdTimeLibraryImpl;
import ai.datasqrl.function.builtin.time.StringToTimestampFunction;
import ai.datasqrl.function.builtin.time.TimestampToEpochFunction;
import ai.datasqrl.function.builtin.time.TimestampToStringFunction;
import ai.datasqrl.function.builtin.time.ToUtcFunction;
import java.time.Instant;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.fun.Now;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class SqrlOperatorTable extends SqlStdOperatorTable {

  private static SqrlOperatorTable instance;

  //SQRL functions here:
  public static final SqlMyFunction MY_FUNCTION = new SqlMyFunction();
  public static final SqlFunction NOW = new Now();
  // conversion
  public static final NumToTimestampFunction NUM_TO_TIMESTAMP = new NumToTimestampFunction();
  public static final TimestampToEpochFunction TIMESTAMP_TO_EPOCH = new TimestampToEpochFunction();
  public static final StringToTimestampFunction STRING_TO_TIMESTAMP = new StringToTimestampFunction();
  public static final TimestampToStringFunction TIMESTAMP_TO_STRING = new TimestampToStringFunction();
  public static final ToUtcFunction TO_UTC = new ToUtcFunction();
  public static final AtZoneFunction AT_ZONE = new AtZoneFunction();
  // rounding
  public static final SqrlRoundingFunction ROUND_TO_SECOND = new SqrlRoundingFunction("ROUND_TO_SECOND",
      ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "roundToSecond", Instant.class)));
  public static final SqrlRoundingFunction ROUND_TO_MINUTE = new SqrlRoundingFunction("ROUND_TO_MINUTE",
      ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "roundToMinute", Instant.class)));
  public static final SqrlRoundingFunction ROUND_TO_HOUR = new SqrlRoundingFunction("ROUND_TO_HOUR",
      ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "roundToHour", Instant.class)));
  public static final SqrlRoundingFunction ROUND_TO_DAY = new SqrlRoundingFunction("ROUND_TO_DAY",
      ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "roundToDay", Instant.class)));
  public static final SqrlRoundingFunction ROUND_TO_MONTH = new SqrlRoundingFunction("ROUND_TO_MONTH",
      ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "roundToMonth", Instant.class)));
  public static final SqrlRoundingFunction ROUND_TO_YEAR = new SqrlRoundingFunction("ROUND_TO_YEAR",
      ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "roundToYear", Instant.class)));
  // getters
  public static final SqrlGettersFunction GET_SECOND = new SqrlGettersFunction("GET_SECOND",
      ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "getSecond", Instant.class)));
  public static final SqrlGettersFunction GET_MINUTE = new SqrlGettersFunction("GET_MINUTE",
      ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "getMinute", Instant.class)));
  public static final SqrlGettersFunction GET_HOUR = new SqrlGettersFunction("GET_HOUR",
      ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "getHour", Instant.class)));
  public static final SqrlGettersFunction GET_DAY_OF_WEEK = new SqrlGettersFunction("GET_DAY_OF_WEEK",
      ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "getDayOfWeek", Instant.class)));
  public static final SqrlGettersFunction GET_DAY_OF_MONTH = new SqrlGettersFunction("GET_DAY_OF_MONTH",
      ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "getDayOfMonth", Instant.class)));
  public static final SqrlGettersFunction GET_DAY_OF_YEAR = new SqrlGettersFunction("GET_DAY_OF_YEAR",
      ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "getDayOfYear", Instant.class)));
  public static final SqrlGettersFunction GET_MONTH = new SqrlGettersFunction("GET_MONTH",
      ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "getMonth", Instant.class)));
  public static final SqrlGettersFunction GET_YEAR = new SqrlGettersFunction("GET_YEAR",
      ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "getYear", Instant.class)));

  public static synchronized SqrlOperatorTable instance() {
    if (instance == null) {
      instance = new SqrlOperatorTable();
      instance.init();
    }

    return instance;
  }

}
