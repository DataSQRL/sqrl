package com.datasqrl.util;

import com.datasqrl.error.ErrorLabel;
import com.datasqrl.parse.SqrlAstException;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.function.Supplier;

public class CheckUtil {

  public static void checkState(SqlNode node, boolean check, String message, String... format) {
    if (!check) {
      throw createAstException(ErrorLabel.GENERIC, ()->node.getParserPosition(),
          ()->String.format(message, format));
    }
  }

  public static <X extends RuntimeException> X fatal(SqlNode node, String message, String... format) {
    throw createAstException(ErrorLabel.GENERIC, ()->node.getParserPosition(), ()->String.format(message, format));
  }

  public static RuntimeException createAstException(ErrorLabel label, Supplier<SqlParserPos> pos,
                                                    Supplier<String> message) {
    return new SqrlAstException(label, pos.get(), message.get());
  }

  public static RuntimeException createAstException(ErrorLabel label, SqlNode node,
                                                    String message) {
    return new SqrlAstException(label, node.getParserPosition(), message);
  }

}