package ai.dataeng.sqml.ingest;

import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.type.Type;
import com.google.common.base.Preconditions;
import lombok.ToString;
import lombok.Value;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Value
public class RelationStats implements Serializable {

    public static final RelationStats EMPTY = new RelationStats(0, Collections.EMPTY_MAP);

    long count;
    Map<String,FieldStats> fieldStats;

    public void collectSchema(SourceTableSchema.Builder builder) {
        for (Map.Entry<String, FieldStats> fieldEntry : fieldStats.entrySet()) {
            String fieldname = fieldEntry.getKey();
            FieldStats fieldstats = fieldEntry.getValue();
            fieldstats.collectSchema(builder, fieldname);
        }
    }


    @ToString
    public static class Accumulator implements org.apache.flink.api.common.accumulators.Accumulator<Map<String,Object>,RelationStats> {

        static final int INITIAL_CAPACITY = 8;

        private Map<String,FieldStats.Accumulator> fieldAccums;
        private long count=0;

        public Accumulator() {
            fieldAccums = new HashMap<>(INITIAL_CAPACITY);
            count = 0;
        }

        public long getCount() {
            return count;
        }

        @Override
        public void add(Map<String, Object> value) {
            Preconditions.checkArgument(value!=null && !value.isEmpty(),"Invalid value");
            //We use lazy initialization to save space since most fields are scalars.
            count++;
            for (Map.Entry<String,Object> entry : value.entrySet()) {
                String normalizedName = QualifiedName.normalizeName(entry.getKey());
                FieldStats.Accumulator fieldAccum = fieldAccums.get(normalizedName);
                if (fieldAccum==null) {
                    fieldAccum = new FieldStats.Accumulator();
                    fieldAccums.put(normalizedName,fieldAccum);
                }
                fieldAccum.add(entry.getValue());
            }
        }

        @Override
        public RelationStats getLocalValue() {
            return new RelationStats(count, fieldAccums.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey,e -> e.getValue().getLocalValue())));
        }

        @Override
        public void resetLocal() {
            count = 0;
            fieldAccums.clear();
        }

        @Override
        public void merge(org.apache.flink.api.common.accumulators.Accumulator<Map<String, Object>, RelationStats> accumulator) {
            Accumulator acc = (Accumulator) accumulator;
            count += acc.count;
            acc.fieldAccums.forEach((k, v) -> {
                FieldStats.Accumulator fieldaccum = fieldAccums.get(k);
                if (fieldaccum == null) fieldAccums.put(k, v.clone());
                else fieldaccum.merge(v);
            });
        }

        @Override
        public Accumulator clone() {
            Accumulator newAccum = new Accumulator();
            newAccum.count = count;
            fieldAccums.forEach((k,v) -> newAccum.fieldAccums.put(k,v.clone()));
            return newAccum;
        }
    }

}
