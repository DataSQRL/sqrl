package ai.dataeng.sqml.schema2.basic;

import java.util.UUID;

public class UuidType extends AbstractBasicType<UUID> {

    public static final UuidType INSTANCE = new UuidType();

    UuidType() {
        super("UUID", UUID.class, s -> UUID.fromString(s));
    }
}
