package ai.datasqrl.config.engines;

import ai.datasqrl.config.provider.DatabaseEngineProvider;
import ai.datasqrl.config.provider.StreamEngineProvider;
import java.io.Serializable;

public interface EngineConfiguration extends Serializable {

  enum Type {STREAM, DATABASE}

  Type getType();

  interface Stream extends EngineConfiguration, StreamEngineProvider {

    default Type getType() {
      return Type.STREAM;
    }

  }

  interface Database extends EngineConfiguration, DatabaseEngineProvider {

    default Type getType() {
      return Type.DATABASE;
    }

  }


}
