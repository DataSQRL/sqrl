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
package com.datasqrl.schema.type;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.schema.flexible.type.basic.TimestampType;
import java.time.Instant;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class BasicTypeTest {

  @Test
  @Disabled
  void dateTimeParsing() {
    String[] timeStrs = {
      "2022-07-15 10:15:30",
      "2022-07-15 10:15:30.543",
      "2022-07-15T10:15:30Z",
      "2011-12-03T10:15:30+01:00",
      "2011-12-03T10:15:30"
    };
    String[] resultTimes = {
      "2022-07-15T17:15:30Z",
      "2022-07-15T17:15:30.543Z",
      "2022-07-15T10:15:30Z",
      "2011-12-03T09:15:30Z",
      "2011-12-03T18:15:30Z"
    };
    for (var i = 0; i < timeStrs.length; i++) {
      var timeStr = timeStrs[i];
      assertThat(TimestampType.INSTANCE.conversion().detectType(timeStr)).as(timeStr).isTrue();
      var errors = ErrorCollector.root();
      var result = TimestampType.INSTANCE.conversion().parseDetected(timeStr, errors);
      assertThat(result).isPresent().contains(Instant.parse(resultTimes[i]));
      //      System.out.println(result.get());
      assertThat(errors.hasErrorsWarningsOrNotices()).isFalse();
    }
  }
}
