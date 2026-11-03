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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.server.config.JdbcConfig;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.PoolOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcClientsConfigTest {

  private Vertx vertx;

  @BeforeEach
  void setUp() {
    vertx = Vertx.vertx();
  }

  @AfterEach
  void tearDown() throws Exception {
    await(vertx.close());
  }

  @Test
  void givenPoolSizeTwo_whenTwoConnectionsHeldAtOnce_thenBothUseOneDatabase() throws Exception {
    var conf = new JdbcConfig.DuckDbConfig();
    conf.setUrl("jdbc:duckdb:");
    conf.setPoolSize(2);
    var pool = JdbcClientsConfig.createDuckDbPool(vertx, conf, new PoolOptions());

    var first = await(pool.getConnection());
    await(first.query("CREATE TABLE t AS SELECT 42 AS v").execute());
    // A pool of one connection would time out here, because first is still held
    var second = await(pool.getConnection());
    var rows = await(second.query("SELECT v FROM t").execute());

    assertThat(rows.size()).isEqualTo(1);
    assertThat(rows.iterator().next().getInteger("v")).isEqualTo(42);

    await(first.close());
    await(second.close());
    await(pool.close());
  }

  private static <T> T await(Future<T> future) throws Exception {
    return future.toCompletionStage().toCompletableFuture().get(10, SECONDS);
  }
}
