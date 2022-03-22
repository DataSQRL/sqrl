package ai.dataeng.sqml.io.sources.stats;

import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import ai.dataeng.sqml.type.basic.ProcessMessage.ProcessBundle;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RelationStats implements Accumulator<Map<String,Object>,RelationStats, NameCanonicalizer> {

    public static final RelationStats EMPTY = new RelationStats(0, Collections.EMPTY_MAP);
    private static final int INITIAL_CAPACITY = 8;

    long count;
    Map<Name,FieldStats> fieldStats;

    public RelationStats() {
        this.fieldStats = new HashMap<>(INITIAL_CAPACITY);
        this.count = 0;
    }

    private RelationStats(long count, Map<Name,FieldStats> fieldStats) {
        this.count = count;
        this.fieldStats = fieldStats;
    }

    public RelationStats clone() {
        RelationStats copy = new RelationStats();
        copy.merge(this);
        return copy;
    }

    public long getCount() {
        return count;
    }

    public static void validate(Map<String, Object> value, DocumentPath path, ProcessBundle<StatsIngestError> errors,
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

    void add(Name name, FieldStats field) {
        Preconditions.checkNotNull(!fieldStats.containsKey(name));
        fieldStats.put(name,field);
    }

    @Override
    public void add(Map<String, Object> value, NameCanonicalizer canonicalizer) {
        count++;
        for (Map.Entry<String,Object> entry : value.entrySet()) {
            Name name = Name.of(entry.getKey(), canonicalizer);
            FieldStats fieldAccum = fieldStats.get(name);
            if (fieldAccum==null) {
                fieldAccum = new FieldStats();
                fieldStats.put(name,fieldAccum);
            }
            fieldAccum.add(entry.getValue(), entry.getKey(), canonicalizer);
        }
    }

    @Override
    public void merge(RelationStats acc) {
        count += acc.count;
        acc.fieldStats.forEach((k, v) -> {
            FieldStats fieldaccum = fieldStats.get(k);
            if (fieldaccum == null) {
                fieldaccum = new FieldStats();
                fieldStats.put(k, fieldaccum);
            }
            fieldaccum.merge(v);
        });
    }

}
