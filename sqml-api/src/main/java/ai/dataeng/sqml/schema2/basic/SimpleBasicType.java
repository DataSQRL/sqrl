package ai.dataeng.sqml.schema2.basic;

import lombok.NonNull;

import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

public abstract class SimpleBasicType<J> extends AbstractBasicType<J> {

    protected abstract Class<J> getJavaClass();

    protected abstract Function<String,J> getStringParser();

    @Override
    public TypeConversion<J> conversion() {
        return new Conversion<>(getJavaClass(),getStringParser());
    }

    public static class Conversion<J> implements TypeConversion<J> {

        private final Class<J> clazz;
        private final Function<String,J> stringParser;

        public Conversion(@NonNull Class<J> clazz, @NonNull Function<String, J> stringParser) {
            this.clazz = clazz;
            this.stringParser = stringParser;
        }

        @Override
        public Set<Class> getJavaTypes() {
            return Collections.singleton(clazz);
        }

        public boolean isInferredType(String original) {
            try {
                stringParser.apply(original);
                return true;
            } catch (IllegalArgumentException e) {
                return false;
            } catch (DateTimeParseException e) {
                return false;
            }
        }

        public ConversionResult<J, ConversionError> parse(Object original) {
            if (original instanceof String) {
                try {
                    J result = stringParser.apply((String) original);
                    return ConversionResult.of(result);
                } catch (IllegalArgumentException e) {
                    return ConversionResult.fatal("Could not parse value [%s] to data type [%s]", original, clazz);
                } catch (DateTimeParseException e) {
                    return ConversionResult.fatal("Could not parse value [%s] to data type [%s]", original, clazz);
                }
            }
            return ConversionResult.fatal("Cannot convert [%s]", original);
        }

    }

}
