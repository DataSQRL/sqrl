package ai.dataeng.sqml.schema2.basic;

import java.util.function.Function;

public class BooleanType extends AbstractBasicType<Boolean> {

    public static final BooleanType INSTANCE = new BooleanType();

    private static Function<String,Boolean> parseBoolean = new Function<String, Boolean>() {
        @Override
        public Boolean apply(String s) {
            if (s.equalsIgnoreCase("true")) return true;
            else if (s.equalsIgnoreCase("false")) return false;
            throw new IllegalArgumentException("Not a boolean");
        }
    };

    BooleanType() {
        super("BOOLEAN", Boolean.class, parseBoolean);
    }
}
