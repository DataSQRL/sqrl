package ai.dataeng.sqml.schema2.basic;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import lombok.NonNull;

import java.util.Set;
import java.util.function.Function;

public class BooleanType extends AbstractBasicType<Boolean> {

    public static final BooleanType INSTANCE = new BooleanType();

    @Override
    public BasicType parentType() {
        return IntegerType.INSTANCE;
    }

    BooleanType() {
        super("BOOLEAN", new Conversion());
    }

    public static class Conversion extends AbstractBasicType.Conversion<Boolean> {

        private static final Set<Class> INT_CLASSES = ImmutableSet.of(Integer.class, Long.class, Byte.class, Short.class);

        public Conversion() {
            super(Boolean.class, parseBoolean);
        }

        public Object cast2Parent(@NonNull Boolean o) {
            return o?1:0;
        }

    }

    private static Function<String,Boolean> parseBoolean = new Function<String, Boolean>() {
        @Override
        public Boolean apply(String s) {
            if (s.equalsIgnoreCase("true")) return true;
            else if (s.equalsIgnoreCase("false")) return false;
            throw new IllegalArgumentException("Not a boolean");
        }
    };
}
