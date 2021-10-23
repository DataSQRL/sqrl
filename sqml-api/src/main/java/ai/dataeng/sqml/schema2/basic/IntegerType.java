package ai.dataeng.sqml.schema2.basic;

import ai.dataeng.sqml.type.SqmlTypeVisitor;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class IntegerType extends AbstractBasicType<Long> {

    public static final IntegerType INSTANCE = new IntegerType();

    @Override
    public String getName() {
        return "INTEGER";
    }

    public BasicType parentType() {
        return NumberType.INSTANCE;
    }

    @Override
    public TypeConversion<Long> conversion() {
        return new Conversion();
    }

    public static class Conversion extends SimpleBasicType.Conversion<Long> {

        private static final Set<Class> INT_CLASSES = ImmutableSet.of(Integer.class, Long.class, Byte.class, Short.class);

        public Conversion() {
            super(Long.class, s -> Long.parseLong(s));
        }

        @Override
        public Set<Class> getJavaTypes() {
            return INT_CLASSES;
        }

        public Long convert(Object o) {
            if (o instanceof Long) return (Long)o;
            if (o instanceof Number) return ((Number)o).longValue();
            if (o instanceof Boolean) return ((Boolean)o).booleanValue()?1L:0L;
            throw new IllegalArgumentException("Invalid type to convert: " + o.getClass());
        }
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
        return visitor.visitIntegerType(this, context);
    }
}
