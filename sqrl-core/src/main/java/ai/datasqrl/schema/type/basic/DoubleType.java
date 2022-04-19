package ai.datasqrl.schema.type.basic;

import ai.datasqrl.schema.type.SqmlTypeVisitor;
import com.google.common.collect.ImmutableSet;
import java.util.Set;

public class DoubleType extends AbstractBasicType<Float> {

    public static final DoubleType INSTANCE = new DoubleType();

    @Override
    public String getName() {
        return "DOUBLE";
    }

    @Override
    public BasicType parentType() {
        return NumberType.INSTANCE;
    }

    @Override
    public TypeConversion<Float> conversion() {
        return new Conversion();
    }

    public static class Conversion extends SimpleBasicType.Conversion<Float> {

        private static final Set<Class> DOUBLE_CLASSES = ImmutableSet.of(Double.class);

        public Conversion() {
            super(Float.class, s -> Float.parseFloat(s));
        }

        @Override
        public Set<Class> getJavaTypes() {
            return DOUBLE_CLASSES;
        }

        public Float convert(Object o) {
            return convertInternal(o);
        }

        public static Float convertInternal(Object o) {
            if (o instanceof Double) return ((Double)o).floatValue();
            if (o instanceof Float) return (Float)o;
            if (o instanceof Number) return ((Number)o).floatValue();
            if (o instanceof Boolean) return ((Boolean)o).booleanValue()?1.0f:0.0f;
            throw new IllegalArgumentException("Invalid type to convert: " + o.getClass());
        }
    }

    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
        return visitor.visitDoubleType(this, context);
    }
}
