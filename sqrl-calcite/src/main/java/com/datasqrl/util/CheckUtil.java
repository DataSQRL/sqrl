package com.datasqrl.util;

import java.util.Optional;
import java.util.function.Supplier;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.datasqrl.error.ErrorLabel;
import com.datasqrl.parse.SqrlAstException;

import lombok.experimental.UtilityClass;

@UtilityClass
public class CheckUtil {

  public static RuntimeException createAstException(Optional<Throwable> cause, ErrorLabel label, Supplier<SqlParserPos> pos,
                                                    Supplier<String> message) {
    return new SqrlAstException(cause, label, pos.get(), message.get());
  }

  public static RuntimeException createAstException(ErrorLabel label, SqlNode node,
                                                    String message) {
    return new SqrlAstException(Optional.empty(), label, node.getParserPosition(), message);
  }

}