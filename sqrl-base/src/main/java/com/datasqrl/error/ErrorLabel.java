package com.datasqrl.error;

import com.google.common.io.Resources;
import java.nio.charset.Charset;
import java.util.function.Function;
import lombok.SneakyThrows;

public interface ErrorLabel {

  String getLabel();

  default String getErrorDescription() {
    return readErrorMessage(this.getLabel().toLowerCase() + MSG_FILE_EXTENSION);
  }

  default Function<String,RuntimeException> toException() {
    return IllegalArgumentException::new;
  }

  String MSG_FILE_EXTENSION = ".md";

  @SneakyThrows
  static String readErrorMessage(String fileName) {
    return Resources.toString(Resources.getResource("errorCodes/" + fileName),
        Charset.defaultCharset());
  }

  public static final ErrorLabel GENERIC = new ErrorLabel() {
    @Override
    public String getLabel() {
      return "GENERIC_ERROR";
    }

    @Override
    public String getErrorDescription() {
      return "";
    }
  };

}
