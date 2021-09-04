package ai.dataeng.sqml.schema2.basic;

public class IntegerType extends NumberType {

    public static final IntegerType INSTANCE = new IntegerType();

    IntegerType() {
        super("INTEGER");
    }
}
