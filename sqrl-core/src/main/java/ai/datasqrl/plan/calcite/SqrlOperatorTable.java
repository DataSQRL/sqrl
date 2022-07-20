package ai.datasqrl.plan.calcite;

import ai.datasqrl.function.builtin.time.*;
import ai.datasqrl.function.builtin.example.SqlMyFunction;
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
  // rounding
  public static final RoundToSecondFunction ROUND_TO_SECOND = new RoundToSecondFunction();
  public static final RoundToMinuteFunction ROUND_TO_MINUTE = new RoundToMinuteFunction();
  public static final RoundToHourFunction ROUND_TO_HOUR = new RoundToHourFunction();
  public static final RoundToDayFunction ROUND_TO_DAY = new RoundToDayFunction();
  public static final RoundToMonthFunction ROUND_TO_MONTH = new RoundToMonthFunction();
  public static final RoundToYearFunction ROUND_TO_YEAR = new RoundToYearFunction();
  // getters
  public static final GetSecondFunction GET_SECOND = new GetSecondFunction();
  public static final GetMinuteFunction GET_MINUTE = new GetMinuteFunction();
  public static final GetHourFunction GET_HOUR = new GetHourFunction();
  public static final GetDayOfWeekFunction GET_DAY_OF_WEEK = new GetDayOfWeekFunction();
  public static final GetDayOfMonthFunction GET_DAY_OF_MONTH = new GetDayOfMonthFunction();
  public static final GetDayOfYearFunction GET_DAY_OF_YEAR = new GetDayOfYearFunction();
  public static final GetMonthFunction GET_MONTH = new GetMonthFunction();
  public static final GetYearFunction GET_YEAR = new GetYearFunction();

  public static synchronized SqrlOperatorTable instance() {
    if (instance == null) {
      instance = new SqrlOperatorTable();
      instance.init();
    }

    return instance;
  }

}
