package ai.dataeng.sqml.config.engines;

public interface EngineConfiguration {

    public static enum Type { STREAM, DATABASE }

    Type getType();

    public interface Stream extends EngineConfiguration {

        default Type getType() {
            return Type.STREAM;
        }

    }

    public interface Database extends EngineConfiguration {

        default Type getType() {
            return Type.DATABASE;
        }

    }


}
