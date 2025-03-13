/*
 * Copyright © 2024 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
