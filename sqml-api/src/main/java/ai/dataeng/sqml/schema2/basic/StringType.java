package ai.dataeng.sqml.schema2.basic;

public class StringType extends AbstractBasicType {

    public static final StringType INSTANCE = new StringType();

    StringType() {
        super("STRING");
    }
}
