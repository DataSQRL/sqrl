package ai.dataeng.sqml.schema2.basic;

public class NumberType extends AbstractBasicType {

    public static final NumberType INSTANCE = new NumberType();

    NumberType() {
        super("NUMBER");
    }

    NumberType(String name) {
        super(name);
    }
}
