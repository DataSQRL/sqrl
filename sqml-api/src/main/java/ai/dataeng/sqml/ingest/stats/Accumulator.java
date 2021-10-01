package ai.dataeng.sqml.ingest.stats;

import ai.dataeng.sqml.schema2.basic.ConversionError;

import java.io.Serializable;

public interface Accumulator<V,A, C> extends Serializable {

    public void add(V value, C context);

    public void merge(A accumulator);

}
