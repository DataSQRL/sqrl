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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

class DuckDbDataSourceTest {

  @Test
  void givenEightThreads_whenOpeningConnectionsAtOnce_thenAllShareOneDatabase() {
    assertTimeoutPreemptively(
        Duration.ofSeconds(10),
        () -> {
          try (var ds =
              new DuckDbDataSource("jdbc:duckdb:", "CREATE TABLE seen (thread INTEGER)")) {
            var start = new CountDownLatch(1);
            // All eight connections are open at once, so a pool of separate physical connections
            // could not hand the same one to every thread
            var allOpen = new CyclicBarrier(8);
            var executor = Executors.newFixedThreadPool(8);
            try {
              var inserts = new ArrayList<Future<?>>();
              for (var i = 0; i < 8; i++) {
                var index = i;
                inserts.add(
                    executor.submit(
                        () -> {
                          start.await();
                          try (var conn = ds.getConnection();
                              var stmt = conn.createStatement()) {
                            allOpen.await();
                            stmt.execute("INSERT INTO seen VALUES (" + index + ")");
                          }
                          return null;
                        }));
              }
              start.countDown();
              for (var insert : inserts) {
                insert.get();
              }
            } finally {
              executor.shutdownNow();
            }

            assertThat(queryString(ds, "SELECT count(*) FROM seen")).isEqualTo("8");
          }
        });
  }

  @Test
  void givenInitSqlSettingMemoryLimit_whenDuplicateOpened_thenDuplicateSeesIt() throws Exception {
    try (var ds = new DuckDbDataSource("jdbc:duckdb:", "SET GLOBAL memory_limit='123MB'")) {
      assertThat(queryString(ds, "SELECT current_setting('memory_limit')")).isEqualTo("117.3 MiB");
    }
  }

  @Test
  void givenFailingInitSql_whenCreated_thenThrowsSqlException() {
    assertThatThrownBy(() -> new DuckDbDataSource("jdbc:duckdb:", "SELECT * FROM missing"))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("missing");
  }

  @Test
  void givenClosedDataSource_whenGetConnection_thenThrowsSqlException() throws Exception {
    var ds = new DuckDbDataSource("jdbc:duckdb:", "SELECT 1");
    ds.getConnection().close();
    ds.close();

    assertThatThrownBy(ds::getConnection).isInstanceOf(SQLException.class);
  }

  private static String queryString(DuckDbDataSource ds, String sql) throws SQLException {
    try (var conn = ds.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(sql)) {
      assertThat(rs.next()).isTrue();
      return rs.getString(1);
    }
  }
}
