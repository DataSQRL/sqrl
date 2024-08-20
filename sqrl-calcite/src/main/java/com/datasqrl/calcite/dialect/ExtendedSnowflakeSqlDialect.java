package com.datasqrl.calcite.dialect;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.function.translations.SqlTranslation;
import com.datasqrl.util.ServiceLoaderDiscovery;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.dialect.SnowflakeSqlDialect;

public class ExtendedSnowflakeSqlDialect extends SnowflakeSqlDialect {
  public static final Map<String, SqlTranslation> translationMap = ServiceLoaderDiscovery
      .getAll(SqlTranslation.class)
      .stream().filter(f->f.getDialect() == Dialect.SNOWFLAKE)
      .collect(Collectors.toMap(f->f.getOperator().getName().toLowerCase(), f->f));

  public static final SqlDialect.Context DEFAULT_CONTEXT;
  public static final SqlDialect DEFAULT;

  static {
    DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
        .withDatabaseProduct(DatabaseProduct.SNOWFLAKE)
//        .withIdentifierQuoteString("\"")
//        .withUnquotedCasing(Casing.TO_UPPER)
    ;
    DEFAULT = new ExtendedSnowflakeSqlDialect(DEFAULT_CONTEXT);
  }
  public ExtendedSnowflakeSqlDialect(Context context) {
    super(context);
  }

  @Override
  public void unparseCall(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    if (translationMap.containsKey(call.getOperator().getName().toLowerCase())) {
      translationMap.get(call.getOperator().getName().toLowerCase())
          .unparse(call, writer, leftPrec, rightPrec);
      return;
    }

    super.unparseCall(writer, call, leftPrec, rightPrec);
  }

}
