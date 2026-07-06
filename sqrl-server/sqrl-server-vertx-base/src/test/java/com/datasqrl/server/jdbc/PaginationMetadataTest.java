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
package com.datasqrl.server.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PaginationMetadataTest {

  @Test
  void givenEmptyResult_whenBuildMetadata_thenSinglePageNoRecords() {
    var json = VertxQueryExecutionContext.buildPaginationMetadata(0, 10, 0, null, null);

    assertThat(json.getLong("totalRecords")).isZero();
    assertThat(json.getInteger("pageSize")).isEqualTo(10);
    assertThat(json.getInteger("currentPage")).isEqualTo(1);
    assertThat(json.getInteger("totalPages")).isZero();
    assertThat(json.getBoolean("hasNextPage")).isFalse();
    assertThat(json.getBoolean("hasPreviousPage")).isFalse();
    assertThat(json.getInteger("nextOffset")).isNull();
    assertThat(json.getInteger("prevOffset")).isNull();
  }

  @Test
  void givenFirstPage_whenBuildMetadata_thenHasNextNoPrevious() {
    var json = VertxQueryExecutionContext.buildPaginationMetadata(25, 10, 0, null, null);

    assertThat(json.getInteger("currentPage")).isEqualTo(1);
    assertThat(json.getInteger("totalPages")).isEqualTo(3);
    assertThat(json.getBoolean("hasNextPage")).isTrue();
    assertThat(json.getBoolean("hasPreviousPage")).isFalse();
    assertThat(json.getInteger("nextOffset")).isEqualTo(10);
    assertThat(json.getInteger("prevOffset")).isNull();
  }

  @Test
  void givenMiddlePage_whenBuildMetadata_thenHasBothNeighbours() {
    var json = VertxQueryExecutionContext.buildPaginationMetadata(25, 10, 10, null, null);

    assertThat(json.getInteger("currentPage")).isEqualTo(2);
    assertThat(json.getBoolean("hasNextPage")).isTrue();
    assertThat(json.getBoolean("hasPreviousPage")).isTrue();
    assertThat(json.getInteger("nextOffset")).isEqualTo(20);
    assertThat(json.getInteger("prevOffset")).isZero();
  }

  @Test
  void givenLastPartialPage_whenBuildMetadata_thenNoNextHasPrevious() {
    var json = VertxQueryExecutionContext.buildPaginationMetadata(25, 10, 20, null, null);

    assertThat(json.getInteger("currentPage")).isEqualTo(3);
    assertThat(json.getBoolean("hasNextPage")).isFalse();
    assertThat(json.getBoolean("hasPreviousPage")).isTrue();
    assertThat(json.getInteger("nextOffset")).isNull();
    assertThat(json.getInteger("prevOffset")).isEqualTo(10);
  }

  @Test
  void givenOffsetBeyondTotal_whenBuildMetadata_thenNoNextPage() {
    var json = VertxQueryExecutionContext.buildPaginationMetadata(25, 10, 30, null, null);

    assertThat(json.getBoolean("hasNextPage")).isFalse();
    assertThat(json.getBoolean("hasPreviousPage")).isTrue();
    assertThat(json.getInteger("prevOffset")).isEqualTo(20);
  }

  @Test
  void givenZeroLimit_whenBuildMetadata_thenDoesNotDivideByZero() {
    var json = VertxQueryExecutionContext.buildPaginationMetadata(25, 0, 0, null, null);

    assertThat(json.getInteger("totalPages")).isZero();
    assertThat(json.getInteger("currentPage")).isEqualTo(1);
  }

  @Test
  void givenEventTimes_whenBuildMetadata_thenPassedThrough() {
    var json =
        VertxQueryExecutionContext.buildPaginationMetadata(
            5, 10, 0, "2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z");

    assertThat(json.getString("firstEventTime")).isEqualTo("2024-01-01T00:00:00Z");
    assertThat(json.getString("lastEventTime")).isEqualTo("2024-01-02T00:00:00Z");
  }
}
