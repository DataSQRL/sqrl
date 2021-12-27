package ai.dataeng.sqml.execution.flink.process.aggregates;

import ai.dataeng.sqml.execution.flink.process.AggregationProcess;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.function.Supplier;

public class SimpleCount implements AggregationProcess.Aggregator {

    private long count = 0;

    @Override
    public Update add(@Nullable Object[] append, @Nullable Object[] retraction) {
        assert append ==null || append.length==1;
        assert retraction==null || retraction.length==1;
        long before = count;
        if (append !=null && append[0]!=null) {
            count++;
        }
        if (retraction!=null && retraction[0]!=null) {
            count--;
        }
        return new Update(count,before);
    }

    @Override
    public Long getValue() {
        return count;
    }

    public static class Factory implements Supplier<SimpleCount>, Serializable {

        @Override
        public SimpleCount get() {
            return new SimpleCount();
        }
    }
}
