package ai.dataeng.sqml.schema2.basic;

public class DateTimeType extends AbstractBasicType {

    public static final DateTimeType INSTANCE = new DateTimeType();

    DateTimeType() {
        super("DATETIME");
    }
}
