package ai.dataeng.sqml.schema2.basic;

public class StringType extends AbstractBasicType<String> {

    public static final StringType INSTANCE = new StringType();

    StringType() {
        super("STRING", new Conversion());
    }

    public static class Conversion extends AbstractBasicType.Conversion<String> {

        public Conversion() {
            super(String.class, s -> s);
        }

        public ConversionResult<String, ConversionError> parse(Object original) {
            return ConversionResult.of(original.toString());
        }

        public String convert(Object o) {
            return o.toString();
        }
    }

}
