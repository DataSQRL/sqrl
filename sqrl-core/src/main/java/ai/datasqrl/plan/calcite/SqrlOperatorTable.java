package ai.datasqrl.plan.calcite;

import ai.datasqrl.function.builtin.time.RoundMonth;
import ai.datasqrl.function.builtin.example.SqlMyFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.fun.Now;
import org.apache.calcite.sql.fun.SqlAbstractTimeFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

public class SqrlOperatorTable extends SqlStdOperatorTable {

  private static SqrlOperatorTable instance;

  //SQRL functions here:
  public static final SqlMyFunction MY_FUNCTION = new SqlMyFunction();
  public static final RoundMonth ROUNDTOMONTH = new RoundMonth();
  public static final SqlFunction NOW = new Now();

  public static synchronized SqrlOperatorTable instance() {
    if (instance == null) {
      instance = new SqrlOperatorTable();
      instance.init();
    }

    return instance;
  }

}
