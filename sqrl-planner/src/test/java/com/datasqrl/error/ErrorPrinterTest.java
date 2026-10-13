/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ErrorPrinterTest {

  @Test
  void givenErrorWithCode_whenPrettyPrint_thenPrintsCodeAndErrorReferenceLink() {
    var errors = ErrorCollector.root();
    errors.exception(ErrorCode.MISSING_TABLE, "Object 'Mesages' not found");

    var printed = ErrorPrinter.prettyPrint(errors);

    assertThat(printed)
        .startsWith("[FATAL] [MISSING_TABLE] Object 'Mesages' not found")
        .contains("See https://docs.datasqrl.com/docs/main/errors#missing_table");
  }

  @Test
  void givenGenericError_whenPrettyPrint_thenPrintsMessageOnly() {
    var errors = ErrorCollector.root();
    errors.exception("Something went wrong");

    assertThat(ErrorPrinter.prettyPrint(errors))
        .isEqualToIgnoringWhitespace("[FATAL] Something went wrong")
        .doesNotContain("errors#");
  }

  @Test
  void givenEveryErrorCode_whenGetErrorDescription_thenDescriptionExists() {
    for (var code : ErrorCode.values()) {
      assertThat(code.getErrorDescription()).as(code.name()).isNotBlank();
    }
  }
}
