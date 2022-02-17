package ai.dataeng.sqml.io.sources.stats;

import java.io.Serializable;

public interface Accumulator<V,A, C> extends Serializable {

    public void add(V value, C context);

    public void merge(A accumulator);

}
