package ai.dataeng.sqml.type.basic;

import ai.dataeng.sqml.type.SqmlTypeVisitor;
import java.util.Collections;
import java.util.Set;

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
            return FloatType.Conversion.convertInternal(o);
        }
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
        return visitor.visitNumberType(this, context);
    }
}
