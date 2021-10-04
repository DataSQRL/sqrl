package ai.dataeng.sqml;

import ai.dataeng.sqml.env.SqmlEnv;
import java.sql.Connection;
import lombok.SneakyThrows;

class GraphqlSqmlContext {

    private final SqmlEnv env;

    public GraphqlSqmlContext(SqmlEnv env) {
      this.env = env;
    }

    @SneakyThrows
    public Connection getConnection() {
      return env.getConnectionProvider()
          .getOrEstablishConnection();
    }
  }