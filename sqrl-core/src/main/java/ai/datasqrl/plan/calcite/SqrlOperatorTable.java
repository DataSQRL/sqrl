package ai.datasqrl.plan.calcite;

import ai.datasqrl.function.builtin.time.*;
import ai.datasqrl.function.builtin.example.SqlMyFunction;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.fun.Now;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.time.Instant;

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
  public static final MakeTimestampFunction MAKE_TIMESTAMP = new MakeTimestampFunction();
  // rounding
  public static final SqrlRoundingFunction ROUND_TO_SECOND = new SqrlRoundingFunction("SECOND",
          ScalarFunctionImpl.create(Types.lookupMethod(SqrlRounding.class, "roundToSecond", Instant.class)));
  public static final SqrlRoundingFunction ROUND_TO_MINUTE = new SqrlRoundingFunction("MINUTE",
          ScalarFunctionImpl.create(Types.lookupMethod(SqrlRounding.class, "roundToMinute", Instant.class)));
  public static final SqrlRoundingFunction ROUND_TO_HOUR = new SqrlRoundingFunction("HOUR",
          ScalarFunctionImpl.create(Types.lookupMethod(SqrlRounding.class, "roundToHour", Instant.class)));
  public static final SqrlRoundingFunction ROUND_TO_DAY = new SqrlRoundingFunction("DAY",
          ScalarFunctionImpl.create(Types.lookupMethod(SqrlRounding.class, "roundToDay", Instant.class)));
  public static final SqrlRoundingFunction ROUND_TO_MONTH = new SqrlRoundingFunction("MONTH",
          ScalarFunctionImpl.create(Types.lookupMethod(SqrlRounding.class, "roundToMonth", Instant.class)));
  public static final SqrlRoundingFunction ROUND_TO_YEAR = new SqrlRoundingFunction("YEAR",
          ScalarFunctionImpl.create(Types.lookupMethod(SqrlRounding.class, "roundToYear", Instant.class)));
  // getters
  public static final SqrlGettersFunction GET_SECOND = new SqrlGettersFunction("SECOND",
          ScalarFunctionImpl.create(Types.lookupMethod(SqrlGetters.class, "getSecond", Instant.class)));
  public static final SqrlGettersFunction GET_MINUTE = new SqrlGettersFunction("MINUTE",
          ScalarFunctionImpl.create(Types.lookupMethod(SqrlGetters.class, "getMinute", Instant.class)));
  public static final SqrlGettersFunction GET_HOUR = new SqrlGettersFunction("HOUR",
          ScalarFunctionImpl.create(Types.lookupMethod(SqrlGetters.class, "getHour", Instant.class)));
  public static final SqrlGettersFunction GET_DAY_OF_WEEK = new SqrlGettersFunction("DAY_OF_WEEK",
          ScalarFunctionImpl.create(Types.lookupMethod(SqrlGetters.class, "getDayOfWeek", Instant.class)));
  public static final SqrlGettersFunction GET_DAY_OF_MONTH = new SqrlGettersFunction("DAY_OF_MONTH",
          ScalarFunctionImpl.create(Types.lookupMethod(SqrlGetters.class, "getDayOfMonth", Instant.class)));
  public static final SqrlGettersFunction GET_DAY_OF_YEAR = new SqrlGettersFunction("DAY_OF_YEAR",
          ScalarFunctionImpl.create(Types.lookupMethod(SqrlGetters.class, "getDayOfYear", Instant.class)));
  public static final SqrlGettersFunction GET_MONTH = new SqrlGettersFunction("MONTH",
          ScalarFunctionImpl.create(Types.lookupMethod(SqrlGetters.class, "getMonth", Instant.class)));
  public static final SqrlGettersFunction GET_YEAR = new SqrlGettersFunction("YEAR",
          ScalarFunctionImpl.create(Types.lookupMethod(SqrlGetters.class, "getYear", Instant.class)));

  public static synchronized SqrlOperatorTable instance() {
    if (instance == null) {
      instance = new SqrlOperatorTable();
      instance.init();
    }

    return instance;
  }

}
