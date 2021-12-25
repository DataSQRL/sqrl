package ai.dataeng.sqml.type.basic;

import ai.dataeng.sqml.type.SqmlTypeVisitor;
import com.google.common.collect.ImmutableSet;
import java.math.BigInteger;
import java.util.Set;

public class BigIntegerType extends AbstractBasicType<BigInteger> {

    public static final BigIntegerType INSTANCE = new BigIntegerType();

    @Override
    public String getName() {
        return "BIGINTEGER";
    }

    public BasicType parentType() {
        return NumberType.INSTANCE;
    }

    @Override
    public TypeConversion<BigInteger> conversion() {
        return new Conversion();
    }

    public static class Conversion extends SimpleBasicType.Conversion<BigInteger> {

        private static final Set<Class> INT_CLASSES = ImmutableSet.of(BigInteger.class);

        public Conversion() {
            super(BigInteger.class, BigInteger::new);
        }

        @Override
        public Set<Class> getJavaTypes() {
            return INT_CLASSES;
        }

        public BigInteger convert(Object o) {
            if (o instanceof BigInteger) return (BigInteger)o;
            throw new IllegalArgumentException("Invalid type to convert: " + o.getClass());
        }
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
        return visitor.visitBigIntegerType(this, context);
    }
}
