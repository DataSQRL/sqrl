package ai.datasqrl.plan.calcite;

import ai.datasqrl.function.builtin.example.SqlMyFunction;
import ai.datasqrl.function.builtin.time.*;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.fun.Now;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class SqrlOperatorTable extends SqlStdOperatorTable {

  private static SqrlOperatorTable instance;

  //SQRL functions here:
  public static final SqlFunction NOW = new NowFunction();
  public static final NumToTimestampFunction NUM_TO_TIMESTAMP = new NumToTimestampFunction();
  public static final TimestampToEpochFunction TIMESTAMP_TO_EPOCH = new TimestampToEpochFunction();
  public static final StringToTimestampFunction STRING_TO_TIMESTAMP = new StringToTimestampFunction();
  public static final TimestampToStringFunction TIMESTAMP_TO_STRING = new TimestampToStringFunction();
  public static final ToUtcFunction TO_UTC = new ToUtcFunction();
  public static final AtZoneFunction AT_ZONE = new AtZoneFunction();
  public static final SqrlTimeRoundingFunction ROUND_TO_SECOND = new SqrlTimeRoundingFunction("ROUND_TO_SECOND",
      ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "roundToSecond", Instant.class)), ChronoUnit.SECONDS);
  public static final SqrlTimeRoundingFunction ROUND_TO_MINUTE = new SqrlTimeRoundingFunction("ROUND_TO_MINUTE",
      ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "roundToMinute", Instant.class)), ChronoUnit.MINUTES);
  public static final SqrlTimeRoundingFunction ROUND_TO_HOUR = new SqrlTimeRoundingFunction("ROUND_TO_HOUR",
      ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "roundToHour", Instant.class)), ChronoUnit.HOURS);
  public static final SqrlTimeRoundingFunction ROUND_TO_DAY = new SqrlTimeRoundingFunction("ROUND_TO_DAY",
      ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "roundToDay", Instant.class)), ChronoUnit.DAYS);
  public static final SqrlTimeRoundingFunction ROUND_TO_MONTH = new SqrlTimeRoundingFunction("ROUND_TO_MONTH",
      ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "roundToMonth", Instant.class)), ChronoUnit.MONTHS);
  public static final SqrlTimeRoundingFunction ROUND_TO_YEAR = new SqrlTimeRoundingFunction("ROUND_TO_YEAR",
      ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "roundToYear", Instant.class)), ChronoUnit.YEARS);
  public static final ExtractTimeFieldFunction GET_SECOND = new ExtractTimeFieldFunction("GET_SECOND",
      ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "getSecond", Instant.class)));
  public static final ExtractTimeFieldFunction GET_MINUTE = new ExtractTimeFieldFunction("GET_MINUTE",
      ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "getMinute", Instant.class)));
  public static final ExtractTimeFieldFunction GET_HOUR = new ExtractTimeFieldFunction("GET_HOUR",
      ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "getHour", Instant.class)));
  public static final ExtractTimeFieldFunction GET_DAY_OF_WEEK = new ExtractTimeFieldFunction("GET_DAY_OF_WEEK",
      ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "getDayOfWeek", Instant.class)));
  public static final ExtractTimeFieldFunction GET_DAY_OF_MONTH = new ExtractTimeFieldFunction("GET_DAY_OF_MONTH",
      ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "getDayOfMonth", Instant.class)));
  public static final ExtractTimeFieldFunction GET_DAY_OF_YEAR = new ExtractTimeFieldFunction("GET_DAY_OF_YEAR",
      ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "getDayOfYear", Instant.class)));
  public static final ExtractTimeFieldFunction GET_MONTH = new ExtractTimeFieldFunction("GET_MONTH",
      ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "getMonth", Instant.class)));
  public static final ExtractTimeFieldFunction GET_YEAR = new ExtractTimeFieldFunction("GET_YEAR",
      ScalarFunctionImpl.create(Types.lookupMethod(StdTimeLibraryImpl.class, "getYear", Instant.class)));

  public static synchronized SqrlOperatorTable instance() {
    if (instance == null) {
      instance = new SqrlOperatorTable();
      instance.init();
    }

    return instance;
  }

}
