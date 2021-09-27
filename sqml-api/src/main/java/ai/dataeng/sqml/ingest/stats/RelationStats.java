package ai.dataeng.sqml.ingest.stats;

import ai.dataeng.sqml.ingest.schema.SourceTableSchema;
import ai.dataeng.sqml.schema2.basic.ConversionError;
import ai.dataeng.sqml.schema2.name.Name;
import ai.dataeng.sqml.schema2.name.NameCanonicalizer;
import com.google.common.base.Strings;

import java.util.*;

public class RelationStats implements Accumulator<Map<String,Object>,RelationStats> {

    public static final RelationStats EMPTY = new RelationStats(0, Collections.EMPTY_MAP);
    private static final int INITIAL_CAPACITY = 8;

    long count;
    Map<Name,FieldStats> fieldStats;
    final NameCanonicalizer canonicalizer;

    public RelationStats(NameCanonicalizer canonicalizer) {
        this.fieldStats = new HashMap<>(INITIAL_CAPACITY);
        this.count = 0;
        this.canonicalizer = canonicalizer;
    }

    private RelationStats(long count, Map<Name,FieldStats> fieldStats) {
        this.count = count;
        this.fieldStats = fieldStats;
        this.canonicalizer = null;
    }

    public long getCount() {
        return count;
    }

    public static void validate(Map<String, Object> value, DocumentPath path, ConversionError.Bundle<StatsIngestError> errors,
                                NameCanonicalizer canonicalizer) {
        if (value==null || value.isEmpty()) errors.add(StatsIngestError.fatal(path,"Invalid value: %s", value));
        Set<Name> names = new HashSet<>(value.size());
        for (Map.Entry<String,Object> entry : value.entrySet()) {
            String name = entry.getKey();
            if (Strings.isNullOrEmpty(name)) errors.add(StatsIngestError.fatal(path,"Invalid name: %s", name));
            if (!names.add(Name.of(name,canonicalizer))) errors.add(StatsIngestError.fatal(path,"Duplicate name: %s", name));
            FieldStats.validate(entry.getValue(), path.resolve(name), errors, canonicalizer);
        }
    }

    @Override
    public void add(Map<String, Object> value) {
        count++;
        for (Map.Entry<String,Object> entry : value.entrySet()) {
            Name name = Name.of(entry.getKey(), canonicalizer);
            FieldStats fieldAccum = fieldStats.get(name);
            if (fieldAccum==null) {
                fieldAccum = new FieldStats(canonicalizer);
                fieldStats.put(name,fieldAccum);
            }
            fieldAccum.add(entry.getValue());
        }
    }

    @Override
    public void merge(RelationStats acc) {
        count += acc.count;
        acc.fieldStats.forEach((k, v) -> {
            FieldStats fieldaccum = fieldStats.get(k);
            if (fieldaccum == null) {
                fieldaccum = new FieldStats(canonicalizer);
                fieldStats.put(k, fieldaccum);
            }
            fieldaccum.merge(v);
        });
    }

    public void collectSchema(SourceTableSchema.Builder builder) {
        for (Map.Entry<Name, FieldStats> fieldEntry : fieldStats.entrySet()) {
            FieldStats fieldstats = fieldEntry.getValue();
            fieldstats.collectSchema(builder, fieldEntry.getKey());
        }
    }

}
