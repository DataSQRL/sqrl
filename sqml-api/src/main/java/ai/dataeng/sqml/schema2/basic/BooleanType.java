package ai.dataeng.sqml.schema2.basic;

public class BooleanType extends AbstractBasicType {

    public static final BooleanType INSTANCE = new BooleanType();

    BooleanType() {
        super("BOOLEAN");
    }
}
