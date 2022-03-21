package ai.dataeng.sqml.config.engines;

import ai.dataeng.sqml.config.provider.StreamEngineProvider;
import java.io.Serializable;

public interface EngineConfiguration {

    public static enum Type { STREAM, DATABASE }

    Type getType();

    public interface Stream extends EngineConfiguration, StreamEngineProvider {

        default Type getType() {
            return Type.STREAM;
        }

    }

    public interface Database extends EngineConfiguration, Serializable {

        default Type getType() {
            return Type.DATABASE;
        }

    }


}
