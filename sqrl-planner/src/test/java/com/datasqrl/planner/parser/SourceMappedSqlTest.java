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
import org.junit.jupiter.api.Test;

class SourceMappedSqlTest {

  @Test
  void givenLiteralsAndFragments_whenBuild_thenConcatenatesThemInOrder() {
    var sql =
        SourceMappedSql.builder()
            .append("SELECT ")
            .append(new ParsedObject<>("a, b", new FileLocation(2, 5)))
            .append(" FROM t")
            .build();

    assertThat(sql.getSql()).isEqualTo("SELECT a, b FROM t");
  }

  @Test
  void givenLocationInFragment_whenToSourceLocation_thenMapsIntoTheStatement() {
    var sql =
        SourceMappedSql.builder()
            .append("SELECT ")
            .append(new ParsedObject<>("a, b", new FileLocation(2, 5)))
            .append(" FROM t")
            .build();

    assertThat(sql.toSourceLocation(new FileLocation(1, 8))).contains(new FileLocation(2, 5));
    assertThat(sql.toSourceLocation(new FileLocation(1, 11))).contains(new FileLocation(2, 8));
  }

  @Test
  void givenLocationInLiteral_whenToSourceLocation_thenReturnsEmpty() {
    var sql =
        SourceMappedSql.builder()
            .append("SELECT ")
            .append(new ParsedObject<>("a, b", new FileLocation(2, 5)))
            .append(" FROM t")
            .build();

    assertThat(sql.toSourceLocation(new FileLocation(1, 1))).isEmpty();
    assertThat(sql.toSourceLocation(new FileLocation(1, 18))).isEmpty();
    assertThat(sql.toSourceLocation(new FileLocation(2, 1))).isEmpty();
  }

  @Test
  void givenMultiLineFragments_whenToSourceLocation_thenMapsLinesAndColumns() {
    var sql =
        SourceMappedSql.builder()
            .append("SELECT ")
            .append(new ParsedObject<>("a,\n  b", new FileLocation(3, 10)))
            .append(" FROM t WHERE ")
            .append(new ParsedObject<>("x\n  > 1", new FileLocation(7, 3)))
            .build();

    assertThat(sql.toSourceLocation(new FileLocation(2, 3))).contains(new FileLocation(4, 3));
    assertThat(sql.toSourceLocation(new FileLocation(2, 18))).contains(new FileLocation(7, 3));
    assertThat(sql.toSourceLocation(new FileLocation(3, 5))).contains(new FileLocation(8, 5));
  }
}
