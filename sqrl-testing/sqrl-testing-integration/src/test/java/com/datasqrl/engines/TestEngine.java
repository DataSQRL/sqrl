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

import com.datasqrl.config.PackageJson;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.Getter;

public interface TestEngine {

  <R, C> R accept(TestEngineVisitor<R, C> visitor, C context);

  static Map<String, Function<PackageJson, TestEngine>> ENGINE_REGISTRY = new HashMap<>();

  static void registerEngine(String name, Function<PackageJson, TestEngine> constructor) {
    ENGINE_REGISTRY.put(name.toLowerCase(), constructor);
  }

  static Function<PackageJson, TestEngine> getEngineConstructor(String name) {
    return ENGINE_REGISTRY.get(name.toLowerCase());
  }

  abstract class AbstractTestEngine implements TestEngine {}

  @Getter
  class PostgresTestEngine extends AbstractTestEngine {
    private static final String name = "postgres";

    public PostgresTestEngine(PackageJson packageJson) {}

    @Override
    public <R, C> R accept(TestEngineVisitor<R, C> visitor, C context) {
      return visitor.visit(this, context);
    }
  }

  @Getter
  class PostgresLogTestEngine extends AbstractTestEngine {
    private static final String name = "postgres_log";

    public PostgresLogTestEngine(PackageJson packageJson) {}

    @Override
    public <R, C> R accept(TestEngineVisitor<R, C> visitor, C context) {
      return visitor.visit(this, context);
    }
  }

  @Getter
  class KafkaTestEngine extends AbstractTestEngine {

    static final String name = "kafka";

    public KafkaTestEngine(PackageJson packageJson) {}

    @Override
    public <R, C> R accept(TestEngineVisitor<R, C> visitor, C context) {
      return visitor.visit(this, context);
    }
  }

  @Getter
  class IcebergTestEngine extends AbstractTestEngine {

    static final String name = "iceberg";

    public IcebergTestEngine(PackageJson packageJson) {}

    @Override
    public <R, C> R accept(TestEngineVisitor<R, C> visitor, C context) {
      return visitor.visit(this, context);
    }
  }

  @Getter
  class VertxTestEngine extends AbstractTestEngine {

    static final String name = "vertx";

    public VertxTestEngine(PackageJson packageJson) {}

    @Override
    public <R, C> R accept(TestEngineVisitor<R, C> visitor, C context) {
      return visitor.visit(this, context);
    }
  }

  @Getter
  class SnowflakeTestEngine extends AbstractTestEngine {

    static final String name = "snowflake";

    public SnowflakeTestEngine(PackageJson packageJson) {}

    @Override
    public <R, C> R accept(TestEngineVisitor<R, C> visitor, C context) {
      return visitor.visit(this, context);
    }
  }

  @Getter
  class FlinkTestEngine extends AbstractTestEngine {

    static final String name = "flink";

    public FlinkTestEngine(PackageJson packageJson) {}

    @Override
    public <R, C> R accept(TestEngineVisitor<R, C> visitor, C context) {
      return visitor.visit(this, context);
    }
  }

  @Getter
  class DuckdbTestEngine extends AbstractTestEngine {

    static final String name = "duckdb";

    public DuckdbTestEngine(PackageJson packageJson) {}

    @Override
    public <R, C> R accept(TestEngineVisitor<R, C> visitor, C context) {
      return visitor.visit(this, context);
    }
  }

  @Getter
  class TestTestEngine extends AbstractTestEngine {

    static final String name = "test";

    public TestTestEngine(PackageJson packageJson) {}

    @Override
    public <R, C> R accept(TestEngineVisitor<R, C> visitor, C context) {
      return visitor.visit(this, context);
    }
  }

  class EngineFactory {

    static {
      registerEngine(PostgresTestEngine.name, PostgresTestEngine::new);
      registerEngine(PostgresLogTestEngine.name, PostgresLogTestEngine::new);
      registerEngine(KafkaTestEngine.name, KafkaTestEngine::new);
      registerEngine(IcebergTestEngine.name, IcebergTestEngine::new);
      registerEngine(DuckdbTestEngine.name, DuckdbTestEngine::new);
      registerEngine(TestTestEngine.name, TestTestEngine::new);
      registerEngine(VertxTestEngine.name, VertxTestEngine::new);
      registerEngine(SnowflakeTestEngine.name, SnowflakeTestEngine::new);
      registerEngine(FlinkTestEngine.name, FlinkTestEngine::new);
    }

    public TestEngines create(PackageJson packageJson) {
      List<TestEngine> testEngines = new ArrayList<>();
      for (String engine : packageJson.getEnabledEngines()) {
        var constructor = TestEngine.getEngineConstructor(engine);
        if (constructor != null) {
          testEngines.add(constructor.apply(packageJson));
        } else {
          // Handle unknown engine
          System.err.println("Unknown engine: " + engine);
          throw new IllegalArgumentException("Unknown engine: " + engine);
        }
      }
      return new TestEngines(packageJson, testEngines);
    }

    public TestEngines createAll() {
      List<TestEngine> testEngines = new ArrayList<>();
      for (String engine : List.of("postgres", "kafka", "snowflake")) {
        var constructor = TestEngine.getEngineConstructor(engine);
        if (constructor != null) {
          testEngines.add(constructor.apply(null));
        } else {
          // Handle unknown engine
          System.err.println("Unknown engine: " + engine);
          throw new IllegalArgumentException("Unknown engine: " + engine);
        }
      }
      return new TestEngines(null, testEngines);
    }
  }

  interface TestEngineVisitor<R, C> {
    default R accept(TestEngines testEngines, C context) {
      testEngines.testEngines().forEach(t -> t.accept(this, context));
      return null;
    }

    R visit(PostgresTestEngine engine, C context);

    R visit(PostgresLogTestEngine engine, C context);

    R visit(KafkaTestEngine engine, C context);

    R visit(IcebergTestEngine engine, C context);

    R visit(DuckdbTestEngine engine, C context);

    R visit(VertxTestEngine engine, C context);

    R visit(SnowflakeTestEngine engine, C context);

    R visit(FlinkTestEngine engine, C context);

    R visit(TestTestEngine engine, C context);
  }
}
