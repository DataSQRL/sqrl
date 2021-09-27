package ai.dataeng.sqml.ingest.stats;

import ai.dataeng.sqml.schema2.basic.ConversionError;

import java.io.Serializable;

public interface Accumulator<V,A> extends Serializable {

    public void add(V value);

    public void merge(A accumulator);

}
