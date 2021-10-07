package ai.dataeng.sqml.schema2.basic;

import ai.dataeng.sqml.schema2.type.Null;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import java.util.function.Function;
import lombok.NonNull;

public class NullType extends AbstractBasicType<Null> {

    public static final NullType INSTANCE = new NullType();

    @Override
    public String getName() {
        return "NULL";
    }

    @Override
    public BasicType parentType() {
        return NumberType.INSTANCE;
    }

    @Override
    public TypeConversion<Null> conversion() {
        return new Conversion();
    }

    public static class Conversion extends SimpleBasicType.Conversion<Null> {
        public Conversion() {
            super(Null.class, s -> new Null());
        }
    }
}
