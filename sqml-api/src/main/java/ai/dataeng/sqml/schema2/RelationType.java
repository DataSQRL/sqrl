package ai.dataeng.sqml.schema2;

import ai.dataeng.sqml.schema2.name.Name;
import lombok.Value;

import java.util.LinkedHashMap;
import java.util.stream.Collectors;

@Value
public class RelationType<Context> implements Type {

    //Order of fields is relevant in a relation
    private final LinkedHashMap<Name,Field<?,Context>> fields;

    @Override
    public String toString() {
        return "{" + fields.values().stream().map(f -> f.toString()).collect(Collectors.joining("; ")) + "}";
    }
}
