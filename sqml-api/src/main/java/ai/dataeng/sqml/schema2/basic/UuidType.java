package ai.dataeng.sqml.schema2.basic;

public class UuidType extends AbstractBasicType {

    public static final UuidType INSTANCE = new UuidType();

    UuidType() {
        super("UUID");
    }
}
