package ai.dataeng.sqml.schema2.basic;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import lombok.NonNull;

import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

public class NumberType extends AbstractBasicType<Double> {

    public static final NumberType INSTANCE = new NumberType();

    @Override
    public String getName() {
        return "NUMBER";
    }

    @Override
    public TypeConversion<Double> conversion() {
        return new Conversion();
    }

    public static class Conversion implements TypeConversion<Double> {

        public Conversion() {
        }

        @Override
        public Set<Class> getJavaTypes() {
            return Collections.EMPTY_SET;
        }

        public Double convert(Object o) {
            Preconditions.checkArgument(o instanceof Number);
            return ((Number)o).doubleValue();
        }
    }

}
