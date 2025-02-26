package com.datasqrl.error;

import static com.datasqrl.error.ResourceFileUtil.readResourceFileContents;

import java.util.function.Function;
import lombok.SneakyThrows;

public interface ErrorLabel {

  String getLabel();

  default String getErrorDescription() {
    return readErrorMessage(this.getLabel().toLowerCase() + MSG_FILE_EXTENSION);
  }

  default Function<String, RuntimeException> toException() {
    return IllegalArgumentException::new;
  }

  String MSG_FILE_EXTENSION = ".md";

  @SneakyThrows
  static String readErrorMessage(String fileName) {
    return readResourceFileContents("errorCodes/" + fileName);
  }

  public static final ErrorLabel GENERIC =
      new ErrorLabel() {
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
