package ai.datasqrl.physical;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.spi.JacksonDeserializer;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.NonNull;

import java.io.Serializable;

@JsonIgnoreProperties(value = "engineName", allowGetters = true)
public interface EngineConfiguration extends Serializable {

  String TYPE_KEY = "engineName";

  String getEngineName();

  @JsonIgnore
  ExecutionEngine.Type getEngineType();

  ExecutionEngine initialize(@NonNull ErrorCollector errors);

  class Deserializer extends JacksonDeserializer<EngineConfiguration> {

    public Deserializer() {
      super(EngineConfiguration.class, TYPE_KEY, EngineConfiguration::getEngineName);
    }
  }

//
//  enum Type {STREAM, DATABASE}
//
//  Type getType();
//
//  interface Stream extends EngineConfiguration, StreamEngineProvider {
//
//    default Type getType() {
//      return Type.STREAM;
//    }
//
//  }
//
//  interface Database extends EngineConfiguration, DatabaseEngineProvider {
//
//    default Type getType() {
//      return Type.DATABASE;
//    }
//
//  }


}
