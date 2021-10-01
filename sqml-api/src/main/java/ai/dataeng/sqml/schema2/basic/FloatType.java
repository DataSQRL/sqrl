package ai.dataeng.sqml.schema2.basic;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import lombok.NonNull;

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

        public Object cast2Parent(@NonNull Double o) {
            return o;
        }

        public Double convert(Object o) {
            Preconditions.checkArgument(o instanceof Number);
            return ((Number)o).doubleValue();
        }


    }
}
