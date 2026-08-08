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
  void givenFirstPageWithNext_whenBuildMetadata_thenHasNextNoPrevious() {
    var json = OffsetPageInfoQuery.paginationMetadata(10, 0, true, null, null, null);

    assertThat(json.getInteger("pageSize")).isEqualTo(10);
    assertThat(json.getInteger("currentPage")).isEqualTo(1);
    assertThat(json.getBoolean("hasNextPage")).isTrue();
    assertThat(json.getBoolean("hasPreviousPage")).isFalse();
    assertThat(json.getInteger("nextOffset")).isEqualTo(10);
    assertThat(json.getInteger("prevOffset")).isNull();
  }

  @Test
  void givenMiddlePageWithNext_whenBuildMetadata_thenHasBothNeighbours() {
    var json = OffsetPageInfoQuery.paginationMetadata(10, 10, true, null, null, null);

    assertThat(json.getInteger("currentPage")).isEqualTo(2);
    assertThat(json.getBoolean("hasNextPage")).isTrue();
    assertThat(json.getBoolean("hasPreviousPage")).isTrue();
    assertThat(json.getInteger("nextOffset")).isEqualTo(20);
    assertThat(json.getInteger("prevOffset")).isZero();
  }

  @Test
  void givenLastPage_whenBuildMetadata_thenNoNextHasPrevious() {
    var json = OffsetPageInfoQuery.paginationMetadata(10, 20, false, null, null, null);

    assertThat(json.getInteger("currentPage")).isEqualTo(3);
    assertThat(json.getBoolean("hasNextPage")).isFalse();
    assertThat(json.getInteger("nextOffset")).isNull();
    assertThat(json.getBoolean("hasPreviousPage")).isTrue();
    assertThat(json.getInteger("prevOffset")).isEqualTo(10);
  }

  @Test
  void givenZeroLimit_whenBuildMetadata_thenDoesNotDivideByZero() {
    var json = OffsetPageInfoQuery.paginationMetadata(0, 0, null, null, null, null);

    assertThat(json.getInteger("currentPage")).isEqualTo(1);
  }

  @Test
  void givenEventTimes_whenBuildMetadata_thenPassedThrough() {
    var json =
        OffsetPageInfoQuery.paginationMetadata(
            10, 0, null, "2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z", null);

    assertThat(json.getString("firstEventTime")).isEqualTo("2024-01-01T00:00:00Z");
    assertThat(json.getString("lastEventTime")).isEqualTo("2024-01-02T00:00:00Z");
  }

  @Test
  void givenNoNextPageInfoQueried_whenBuildMetadata_thenNextFieldsOmitted() {
    var json = OffsetPageInfoQuery.paginationMetadata(10, 0, null, null, null, null);

    assertThat(json.containsKey("hasNextPage")).isFalse();
    assertThat(json.containsKey("nextOffset")).isFalse();
    assertThat(json.getInteger("pageSize")).isEqualTo(10);
    assertThat(json.getBoolean("hasPreviousPage")).isFalse();
  }

  @Test
  void givenNoTotalsQueried_whenBuildMetadata_thenTotalFieldsOmitted() {
    var json = OffsetPageInfoQuery.paginationMetadata(10, 0, null, null, null, null);

    assertThat(json.containsKey("totalRecords")).isFalse();
    assertThat(json.containsKey("totalPages")).isFalse();
  }

  @Test
  void givenTotalRecords_whenBuildMetadata_thenTotalPagesRoundedUp() {
    var json = OffsetPageInfoQuery.paginationMetadata(10, 0, null, null, null, 25L);

    assertThat(json.getLong("totalRecords")).isEqualTo(25L);
    assertThat(json.getInteger("totalPages")).isEqualTo(3);
  }

  @Test
  void givenTotalRecordsAndZeroPageSize_whenBuildMetadata_thenDoesNotDivideByZero() {
    var json = OffsetPageInfoQuery.paginationMetadata(0, 0, null, null, null, 25L);

    assertThat(json.getInteger("totalPages")).isZero();
  }
}
