package ai.datasqrl.schema.type.basic;

import ai.datasqrl.schema.type.SqmlTypeVisitor;
import java.util.UUID;
import java.util.function.Function;

public class UuidType extends SimpleBasicType<UUID> {

    public static final UuidType INSTANCE = new UuidType();

    @Override
    public String getName() {
        return "UUID";
    }

    @Override
    protected Class<UUID> getJavaClass() {
        return UUID.class;
    }

    @Override
    protected Function<String, UUID> getStringParser() {
        return s -> UUID.fromString(s);
    }

    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
        return visitor.visitUuidType(this, context);
    }
}
