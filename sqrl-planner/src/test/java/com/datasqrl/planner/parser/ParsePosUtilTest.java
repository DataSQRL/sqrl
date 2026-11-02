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
package com.datasqrl.planner.parser;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.error.ErrorLocation.FileLocation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.junit.jupiter.api.Test;

class ParsePosUtilTest {

  @Test
  void givenValidationErrorWithPositionInMessage_whenConvert_thenUsesThatPosition() {
    var error =
        new TableException(
            "Could not create table",
            new ValidationException(
                "The rowtime attribute field 'ordertime' is not defined in the table schema, at"
                    + " line 4, column 17\nAvailable fields: ['id', 'order_time']"));

    var converted = ParsePosUtil.convertFlinkParserException(error);

    assertThat(converted)
        .hasValueSatisfying(
            messageLocation -> {
              assertThat(messageLocation.location()).isEqualTo(new FileLocation(4, 17));
              assertThat(messageLocation.message())
                  .isEqualTo(
                      "The rowtime attribute field 'ordertime' is not defined in the table schema,"
                          + " Available fields: ['id', 'order_time']");
            });
  }

  @Test
  void givenValidationErrorAtLineZero_whenConvert_thenHasNoPosition() {
    var error =
        new ValidationException(
            "Primary key column 'id' is not defined in the schema at line 0, column 0");

    assertThat(ParsePosUtil.convertFlinkParserException(error)).isEmpty();
  }
}
