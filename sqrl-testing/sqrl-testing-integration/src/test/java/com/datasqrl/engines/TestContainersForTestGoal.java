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
package com.datasqrl.engines;

import static com.datasqrl.env.EnvVariableNames.KAFKA_BOOTSTRAP_SERVERS;
import static com.datasqrl.env.EnvVariableNames.KAFKA_GROUP_ID;
import static com.datasqrl.env.EnvVariableNames.POSTGRES_AUTHORITY;
import static com.datasqrl.env.EnvVariableNames.POSTGRES_DATABASE;
import static com.datasqrl.env.EnvVariableNames.POSTGRES_HOST;
import static com.datasqrl.env.EnvVariableNames.POSTGRES_JDBC_URL;
import static com.datasqrl.env.EnvVariableNames.POSTGRES_PASSWORD;
import static com.datasqrl.env.EnvVariableNames.POSTGRES_PORT;
import static com.datasqrl.env.EnvVariableNames.POSTGRES_USERNAME;

import com.datasqrl.engines.TestContainersForTestGoal.TestContainerHook;
import com.datasqrl.engines.TestEngine.DuckdbTestEngine;
import com.datasqrl.engines.TestEngine.FlinkTestEngine;
import com.datasqrl.engines.TestEngine.IcebergTestEngine;
import com.datasqrl.engines.TestEngine.KafkaTestEngine;
import com.datasqrl.engines.TestEngine.PostgresLogTestEngine;
import com.datasqrl.engines.TestEngine.PostgresTestEngine;
import com.datasqrl.engines.TestEngine.SnowflakeTestEngine;
import com.datasqrl.engines.TestEngine.TestEngineVisitor;
import com.datasqrl.engines.TestEngine.TestTestEngine;
import com.datasqrl.engines.TestEngine.VertxTestEngine;
import com.datasqrl.env.GlobalEnvironmentStore;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

public class TestContainersForTestGoal implements TestEngineVisitor<TestContainerHook, Void> {

  @Override
  public TestContainerHook accept(TestEngines testEngines, Void context) {
    List<TestContainerHook> hooks = new ArrayList<>();
    for (TestEngine engine : testEngines.testEngines()) {
      hooks.add(engine.accept(this, context));
    }

    return new ListTestContainerHook(hooks);
  }

  @Override
  public TestContainerHook visit(PostgresTestEngine engine, Void context) {
    return new TestContainerHook() {
      final PostgreSQLContainer testDatabase =
          new PostgreSQLContainer(
                  DockerImageName.parse("pgvector/pgvector:0.8.1-pg18-trixie")
                      .asCompatibleSubstituteFor("postgres"))
              .withDatabaseName("datasqrl")
              .withUsername("foo")
              .withPassword("secret");

      @Override
      public void start() {
        testDatabase.start();
        GlobalEnvironmentStore.put(POSTGRES_JDBC_URL, testDatabase.getJdbcUrl());
        GlobalEnvironmentStore.put(POSTGRES_AUTHORITY, testDatabase.getJdbcUrl().split("://")[1]);
        GlobalEnvironmentStore.put(POSTGRES_HOST, testDatabase.getHost());
        GlobalEnvironmentStore.put(
            POSTGRES_PORT,
            testDatabase.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT).toString());
        GlobalEnvironmentStore.put(POSTGRES_USERNAME, testDatabase.getUsername());
        GlobalEnvironmentStore.put(POSTGRES_PASSWORD, testDatabase.getPassword());
        GlobalEnvironmentStore.put(POSTGRES_DATABASE, testDatabase.getDatabaseName());

        execStmt("CREATE EXTENSION pgcrypto;");
      }

      @Override
      public void clear() {
        execStmt(
            "DO $$ DECLARE r RECORD; BEGIN FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE'; END LOOP; END $$;");
      }

      @Override
      public void teardown() {
        testDatabase.stop();
      }

      private void execStmt(String sql) {
        try (var conn =
                DriverManager.getConnection(
                    testDatabase.getJdbcUrl(),
                    testDatabase.getUsername(),
                    testDatabase.getPassword());
            var stmt = conn.createStatement()) {

          stmt.execute(sql);

        } catch (SQLException e) {
          throw new RuntimeException("Postgres execution failed", e);
        }
      }
    };
  }

  @Override
  public TestContainerHook visit(PostgresLogTestEngine engine, Void context) {
    return new NoopTestContainerHook();
  }

  @Override
  public TestContainerHook visit(KafkaTestEngine engine, Void context) {
    return new TestContainerHook() {
      // Mirrored to GHCR to avoid rate limiting from docker.redpanda.com
      final RedpandaContainer testKafka =
          new RedpandaContainer("ghcr.io/datasqrl/redpanda:v23.1.2");

      @Override
      public void start() {
        testKafka.start();
        GlobalEnvironmentStore.put(KAFKA_BOOTSTRAP_SERVERS, testKafka.getBootstrapServers());
        GlobalEnvironmentStore.put(KAFKA_GROUP_ID, UUID.randomUUID().toString());
      }

      @Override
      public void clear() {
        var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, testKafka.getBootstrapServers());
        try (var admin = AdminClient.create(props)) {
          // List all topics
          List<String> topics = new ArrayList<>(admin.listTopics().names().get());
          // Delete all topics
          admin.deleteTopics(topics).all().get();
        } catch (Exception e) {
          throw new RuntimeException("Failed to delete topics", e);
        }
      }

      @Override
      public void teardown() {
        testKafka.stop();
      }
    };
  }

  @Override
  public TestContainerHook visit(IcebergTestEngine engine, Void context) {
    return NoopTestContainerHook.INSTANCE;
  }

  @Override
  public TestContainerHook visit(DuckdbTestEngine engine, Void context) {
    return NoopTestContainerHook.INSTANCE;
  }

  @Override
  public TestContainerHook visit(VertxTestEngine engine, Void context) {
    return NoopTestContainerHook.INSTANCE;
  }

  @Override
  public TestContainerHook visit(SnowflakeTestEngine engine, Void context) {
    return new TestContainerHook() {

      @Override
      public void start() {}

      @Override
      public void clear() {}

      @Override
      public void teardown() {}

      @Override
      public Map<String, String> getEnv() {
        return Map.of("SNOWFLAKE_USER", "daniel", "SNOWFLAKE_ID", "ngb00233");
      }
    };
  }

  @Override
  public TestContainerHook visit(FlinkTestEngine engine, Void context) {
    return new TestContainerHook() {
      @Override
      public void start() {}

      @Override
      public void clear() {}

      @Override
      public void teardown() {}

      @Override
      public Map<String, String> getEnv() {
        return System.getenv();
      }
    };
  }

  @Override
  public TestContainerHook visit(TestTestEngine engine, Void context) {
    return new TestContainerHook() {
      @Override
      public void start() {}

      @Override
      public void clear() {}

      @Override
      public void teardown() {}

      @Override
      public Map<String, String> getEnv() {
        return Map.of("FLINK_RESTART_STRATEGY", "test");
      }
    };
  }

  public interface TestContainerHook {
    void start();

    void clear();

    void teardown();

    default Map<String, String> getEnv() {
      return Map.of();
    }
    ;
  }

  public static class NoopTestContainerHook implements TestContainerHook {
    public static final NoopTestContainerHook INSTANCE = new NoopTestContainerHook();

    @Override
    public void start() {}

    @Override
    public void clear() {}

    @Override
    public void teardown() {}
  }

  @AllArgsConstructor
  public static class ListTestContainerHook implements TestContainerHook {
    List<TestContainerHook> hooks;

    @Override
    public void start() {
      for (TestContainerHook hook : hooks) {
        hook.start();
      }
    }

    @Override
    public void clear() {
      for (TestContainerHook hook : hooks) {
        hook.clear();
      }
    }

    @Override
    public void teardown() {
      for (TestContainerHook hook : hooks) {
        hook.teardown();
      }
    }

    @Override
    public Map<String, String> getEnv() {
      Map<String, String> env = new HashMap<>();
      for (TestContainerHook hook : hooks) {
        env.putAll(hook.getEnv());
      }
      return env;
    }
  }
}
