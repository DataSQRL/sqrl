package ai.dataeng.sqml.schema2.basic;

import lombok.NonNull;

import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

public abstract class AbstractBasicType<J> implements BasicType<J> {

    private final String name;
    private final TypeConversion<J> conversion;

    AbstractBasicType(String name, TypeConversion<J> conversion) {
        this.name = name;
        this.conversion = conversion;
    }

    AbstractBasicType(String name, Class<J> clazz, Function<String,J> stringParser) {
        this.name = name;
        this.conversion = new Conversion<>(clazz,stringParser);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public BasicType combine(@NonNull BasicType other) {
        if (other.getName().equals(name)) return this;
        else return null;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractBasicType<?> that = (AbstractBasicType<?>) o;
        return name.equals(that.name);
    }

    @Override
    public TypeConversion<J> conversion() {
        return null;
    }

    @Override
    public String toString() {
        return getName();
    }


    public static class Conversion<J> implements TypeConversion<J> {

        private final Class<J> clazz;
        private final Function<String,J> stringParser;

        public Conversion(Class<J> clazz, Function<String, J> stringParser) {
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
