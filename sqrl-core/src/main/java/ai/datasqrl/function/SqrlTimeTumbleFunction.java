package ai.datasqrl.function;

public interface SqrlTimeTumbleFunction extends SqrlAwareFunction {

    public Specification getSpecification(long[] arguments);

    interface Specification {

        long getBucketWidthMillis();

    }

}
