package ai.dataeng.sqml.schema2.basic;

import lombok.Value;

@Value
public class ParseSignature {

    private final boolean fromString;
    private final boolean fromBinary;
    private final boolean fromTable;

    //How do we distinguish between inferring type and coercing type based on user schema? The former should be a lot more conservative

}
