package ai.dataeng.sqml.schema2.basic;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class FloatType extends NumberType<Double> {

    public static final FloatType INSTANCE = new FloatType();

    FloatType() {
        super("FLOAT", new Conversion());
    }

    public static class Conversion extends AbstractBasicType.Conversion<Double> {

        private static final Set<Class> FLOAT_CLASSES = ImmutableSet.of(Float.class, Double.class);

        public Conversion() {
            super(Double.class, s -> Double.parseDouble(s));
        }

        @Override
        public Set<Class> getJavaTypes() {
            return FLOAT_CLASSES;
        }

        public Double convert(Object o) {
            Preconditions.checkArgument(o instanceof Number);
            return ((Number)o).doubleValue();
        }
    }
}
