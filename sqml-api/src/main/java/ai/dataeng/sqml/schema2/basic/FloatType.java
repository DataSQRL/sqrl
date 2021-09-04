package ai.dataeng.sqml.schema2.basic;

public class FloatType extends NumberType {

    public static final FloatType INSTANCE = new FloatType();

    FloatType() {
        super("FLOAT");
    }
}
