package ai.dataeng.sqml.ingest.stats;

import ai.dataeng.sqml.schema2.Type;
import lombok.NonNull;
import lombok.Value;

public interface TypeSignature {

    public Type getRaw();

    public Type getDetected();

    public int getArrayDepth();

    @Value
    public static class Simple implements TypeSignature {

        @NonNull
        private final Type raw;
        @NonNull
        private final Type detected;
        private final int arrayDepth;

    }
}
