package com.datasqrl.function.translations;

import com.datasqrl.calcite.Dialect;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;

/**
 * Used to implement a function translation to a particular dialect.
 *
 * These are used to rewrite functions during unparsing in {@link org.apache.calcite.sql.SqlDialect#unparseCall(SqlWriter, SqlCall, int, int)}
 * for the various dialect extensions that DataSQRL supports.
 *
 * Note, that {@link SqlTranslation} is for simple function translations. If a translation
 * requires structural changes, use {@link com.datasqrl.calcite.function.OperatorRuleTransform} instead.
 */
public interface SqlTranslation {

  /**
   *
   * @return The dialect for which this translation applies
   */
  Dialect getDialect();

  /**
   *
   * @return The operator that is  translated
   */
  SqlOperator getOperator();

  /**
   * Translates the operator call via unparsing
   * @param call
   * @param writer
   * @param leftPrec
   * @param rightPrec
   */
  void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec);
}
