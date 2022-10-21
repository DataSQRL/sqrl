package ai.datasqrl.physical;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

import java.util.EnumSet;

/**
 * Describes a physical execution engine and it's capabilities.
 */
public interface ExecutionEngine {

    public enum Type {
        STREAM, DATABASE, SERVER;

        public boolean isWrite() {
            return this==STREAM;
        }
        public boolean isRead() {
            return this==DATABASE || this==SERVER;
        }
    }

    boolean supports(EngineCapability capability);

    Type getType();

    @AllArgsConstructor
    @Getter
    public static abstract class Impl implements ExecutionEngine {

        protected final @NonNull Type type;
        protected final @NonNull EnumSet<EngineCapability> capabilities;

        @Override
        public boolean supports(EngineCapability capability) {
            return capabilities.contains(capability);
        }

    }

}
