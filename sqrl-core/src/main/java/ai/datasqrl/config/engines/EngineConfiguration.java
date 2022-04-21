package ai.datasqrl.config.engines;

import ai.datasqrl.config.provider.StreamEngineProvider;
import java.io.Serializable;

public interface EngineConfiguration {

  enum Type {STREAM, DATABASE}

  Type getType();

  interface Stream extends EngineConfiguration, StreamEngineProvider {

    default Type getType() {
      return Type.STREAM;
    }

  }

  interface Database extends EngineConfiguration, Serializable {

    default Type getType() {
      return Type.DATABASE;
    }

  }


}
