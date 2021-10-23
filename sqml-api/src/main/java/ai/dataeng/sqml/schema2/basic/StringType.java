package ai.dataeng.sqml.schema2.basic;

import ai.dataeng.sqml.type.SqmlTypeVisitor;
import lombok.NonNull;

import java.util.Collections;
import java.util.Set;

public class StringType extends AbstractBasicType<String> {

    public static final StringType INSTANCE = new StringType();

    @Override
    public String getName() {
        return "STRING";
    }

    @Override
    public TypeConversion<String> conversion() {
        return new Conversion();
    }

    public static class Conversion implements TypeConversion<String> {

        public Conversion() {
        }

        @Override
        public Set<Class> getJavaTypes() {
            return Collections.singleton(String.class);
        }

        public String convert(Object o) {
            return o.toString();
        }
    }

    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
        return visitor.visitStringType(this, context);
    }
}
