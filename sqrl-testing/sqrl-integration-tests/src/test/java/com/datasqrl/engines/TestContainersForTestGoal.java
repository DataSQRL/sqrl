package com.datasqrl.engines;

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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

public class TestContainersForTestGoal implements TestEngineVisitor<TestContainerHook, Void> {

  @Override
  public TestContainerHook accept(TestEngines testEngines, Void context) {
    List<TestContainerHook> hooks = new ArrayList<>();
    for (TestEngine engine : testEngines.getTestEngines()) {
      hooks.add(engine.accept(this, context));
    }

    return new ListTestContainerHook(hooks);
  }

  @Override
  public TestContainerHook visit(PostgresTestEngine engine, Void context) {
    return new TestContainerHook() {
        PostgreSQLContainer<?> testDatabase = new PostgreSQLContainer<>(
          DockerImageName.parse("ankane/pgvector:v0.5.0")
              .asCompatibleSubstituteFor("postgres"))
          .withDatabaseName("datasqrl")
          .withUsername("foo")
          .withPassword("secret");

      @Override
      public void start() {
        testDatabase.start();
      }

      @Override
      public void clear() {
        try (Connection conn = DriverManager.getConnection(testDatabase.getJdbcUrl(), testDatabase.getUsername(), testDatabase.getPassword());
            Statement stmt = conn.createStatement()) {
          stmt.execute("DO $$ DECLARE r RECORD; BEGIN FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE'; END LOOP; END $$;");
        } catch (SQLException e) {
          throw new RuntimeException("Failed to drop tables", e);
        }
      }

      @Override
      public void teardown() {
        testDatabase.stop();
      }

      @Override
      public Map<String, String> getEnv() {
        Map<String, String> env = new HashMap<>(8);
        env.put("JDBC_URL", testDatabase.getJdbcUrl());
        env.put("JDBC_AUTHORITY", testDatabase.getJdbcUrl().split("://")[1]);
        env.put("PGHOST", testDatabase.getHost());
        env.put("PGUSER", testDatabase.getUsername());
        env.put("JDBC_USERNAME", testDatabase.getUsername());
        env.put("JDBC_PASSWORD", testDatabase.getPassword());
        env.put("PGPORT", testDatabase.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT).toString());
        env.put("PGPASSWORD", testDatabase.getPassword());
        env.put("PGDATABASE", testDatabase.getDatabaseName());
        return env;
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
      RedpandaContainer testKafka = new RedpandaContainer(
          "docker.redpanda.com/redpandadata/redpanda:v23.1.2");

      @Override
      public void start() {
        testKafka.start();
      }

      @Override
      public void clear() {
        Properties props = new Properties();
        props.put("bootstrap.servers", testKafka.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(props)) {
          // List all topics
          List<String> topics = admin.listTopics().names().get().stream().collect(Collectors.toList());
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

      @Override
      public Map<String, String> getEnv() {
        return Map.of("PROPERTIES_BOOTSTRAP_SERVERS", testKafka.getBootstrapServers());
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
      public void start() {

      }

      @Override
      public void clear() {

      }

      @Override
      public void teardown() {

      }

      @Override
      public Map<String, String> getEnv() {
        return Map.of("SNOWFLAKE_USER", "daniel",
            "SNOWFLAKE_ID","ngb00233"
            );
      }
    };
  }

  @Override
  public TestContainerHook visit(FlinkTestEngine engine, Void context) {
    return new TestContainerHook() {
      @Override
      public void start() {

      }

      @Override
      public void clear() {

      }

      @Override
      public void teardown() {

      }

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
      public void start() {

      }

      @Override
      public void clear() {

      }

      @Override
      public void teardown() {

      }

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

    Map<String, String> getEnv();
  }

  public static class NoopTestContainerHook implements TestContainerHook {
    public static final NoopTestContainerHook INSTANCE = new NoopTestContainerHook();
    @Override
    public void start() {

    }

    @Override
    public void clear() {

    }

    @Override
    public void teardown() {

    }

    @Override
    public Map<String, String> getEnv() {
      return Map.of();
    }
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