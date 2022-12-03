package com.datasqrl.function;

public interface SqrlTimeTumbleFunction extends TimestampPreservingFunction {

    public Specification getSpecification(long[] arguments);

    interface Specification {

        long getBucketWidthMillis();

    }

}
