package ai.dataeng.sqml.schema2.basic;

import java.time.OffsetDateTime;

public class DateTimeType extends AbstractBasicType<OffsetDateTime> {

    public static final DateTimeType INSTANCE = new DateTimeType();

    DateTimeType() {
        super("DATETIME", OffsetDateTime.class, s -> OffsetDateTime.parse(s));
    }
}
