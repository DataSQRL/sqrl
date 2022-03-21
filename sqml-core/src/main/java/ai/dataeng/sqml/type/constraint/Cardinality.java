package ai.dataeng.sqml.type.constraint;

import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.type.ArrayType;
import ai.dataeng.sqml.type.Type;
import ai.dataeng.sqml.type.basic.ConversionResult;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;

@Getter
public class Cardinality implements Constraint {

    public static final Name NAME = Name.system("cardianlity");

    public static final Cardinality UNCONSTRAINED = new Cardinality(0,Long.MAX_VALUE);

    private long min;
    private long max;

    private Cardinality() {} //For Kryo

    public Cardinality(long min, long max) {
        Preconditions.checkArgument(min>=0);
        Preconditions.checkArgument(max>=min && max>0);
        this.min = min;
        this.max = max;
    }

    public boolean isSingleton() {
        return max<=1;
    }

    public boolean isNonZero() {
        return min>0;
    }

    @Override
    public boolean satisfies(Object value) {
        Preconditions.checkArgument(value.getClass().isArray());
        long length = ((Object[])value).length;
        return length>=min && length<=max;
    }

    @Override
    public boolean appliesTo(Type type) {
        return type instanceof ArrayType;
    }

    @Override
    public Name getName() {
        return NAME;
    }

    @Override
    public Map<String, Object> export() {
        return Map.of(Factory.KEYS[0], min, Factory.KEYS[1], max);
    }

    @Override
    public String toString() {
        return NAME.getDisplay() + "[" + min + ":" + max + "]";
    }

    public static class Factory implements Constraint.Factory {

        public static final String[] KEYS = {"min", "max"};

        @Override
        public Name getName() {
            return NAME;
        }

        @Override
        public ConversionResult<Constraint, ProcessMessage> create(Map<String, Object> parameters) {
            long[] minmax = new long[2];
            for (int i = 0; i < minmax.length; i++) {
                Object value = parameters.get(KEYS[i]);
                Optional<Long> v = getInt(value);
                if (v.isEmpty()) return ConversionResult.fatal("Invalid integer value [%s] for key [%s]",value, KEYS[i]);
                else minmax[i]=v.get();
            }
            if (minmax[0]<0 || minmax[1]<1 || minmax[0]>minmax[1]) {
                return ConversionResult.fatal("Invalid min [%s] and max [%s] values",minmax[0],minmax[1]);
            }
            return ConversionResult.of(new Cardinality(minmax[0],minmax[1]));
        }

        public static Optional<Long> getInt(Object value) {
            if (value == null || !(value instanceof Number)) return Optional.empty();
            return Optional.of(((Number)value).longValue());
        }

    }
}
