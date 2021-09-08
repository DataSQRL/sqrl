package ai.dataeng.sqml.schema2.basic;

import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

public class NumberType<N extends Number> extends AbstractBasicType<N> {

    public static final NumberType INSTANCE = new NumberType();

    NumberType() {
        super("NUMBER", new TypeConversion<N>() {
            @Override
            public Set<Class> getJavaTypes() {
                return Collections.EMPTY_SET;
            }
        });
    }

    NumberType(String name, TypeConversion<N> c) {
        super(name, c);
    }
}
