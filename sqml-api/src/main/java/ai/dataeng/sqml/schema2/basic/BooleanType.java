package ai.dataeng.sqml.schema2.basic;

import ai.dataeng.sqml.type.SqmlTypeVisitor;
import java.util.function.Function;

public class BooleanType extends AbstractBasicType<Boolean> {

    private static Function<String,Boolean> parseBoolean = new Function<String, Boolean>() {
        @Override
        public Boolean apply(String s) {
            if (s.equalsIgnoreCase("true")) return true;
            else if (s.equalsIgnoreCase("false")) return false;
            throw new IllegalArgumentException("Not a boolean");
        }
    };

    public static final BooleanType INSTANCE = new BooleanType();

    @Override
    public String getName() {
        return "BOOLEAN";
    }

    @Override
    public BasicType parentType() {
        return IntegerType.INSTANCE;
    }

    @Override
    public Conversion conversion() {
        return new Conversion();
    }

    public static class Conversion extends SimpleBasicType.Conversion<Boolean> {

        public Conversion() {
            super(Boolean.class, parseBoolean);
        }

        public Boolean convert(Object o) {
            if (o instanceof Boolean) return (Boolean)o;
            if (o instanceof Number) return ((Number)o).longValue()>0;
            throw new IllegalArgumentException("Invalid type to convert: " + o.getClass());
        }

    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
        return visitor.visitBooleanType(this, context);
    }
}
