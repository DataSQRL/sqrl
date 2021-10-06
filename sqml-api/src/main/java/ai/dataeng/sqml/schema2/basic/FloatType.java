package ai.dataeng.sqml.schema2.basic;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class FloatType extends AbstractBasicType<Double> {

    public static final FloatType INSTANCE = new FloatType();

    @Override
    public String getName() {
        return "FLOAT";
    }

    @Override
    public BasicType parentType() {
        return NumberType.INSTANCE;
    }

    @Override
    public TypeConversion<Double> conversion() {
        return new Conversion();
    }

    public static class Conversion extends SimpleBasicType.Conversion<Double> {

        private static final Set<Class> FLOAT_CLASSES = ImmutableSet.of(Float.class, Double.class);

        public Conversion() {
            super(Double.class, s -> Double.parseDouble(s));
        }

        @Override
        public Set<Class> getJavaTypes() {
            return FLOAT_CLASSES;
        }

        public Double convert(Object o) {
            return convertInternal(o);
        }

        public static Double convertInternal(Object o) {
            if (o instanceof Double) return (Double)o;
            if (o instanceof Number) return ((Number)o).doubleValue();
            if (o instanceof Boolean) return ((Boolean)o).booleanValue()?1.0:0.0;
            throw new IllegalArgumentException("Invalid type to convert: " + o.getClass());
        }


    }
}
