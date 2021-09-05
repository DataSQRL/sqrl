package ai.dataeng.sqml.schema2;

import ai.dataeng.sqml.schema2.name.Name;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RelationType<F extends Field> implements Type {

    protected final List<F> fields;

    protected RelationType(List<F> fields) {
        this.fields = fields;
    }

    //Lazily initialized when requested because this only works for fields with names
    private transient Map<Name,F> fieldsByName = null;

    public F getFieldByName(Name name) {
        if (fieldsByName ==null) {
            fieldsByName = fields.stream().collect(Collectors.toUnmodifiableMap(t -> t.getName(), Function.identity()));
        }
        return fieldsByName.get(name);
    }

    @Override
    public String toString() {
        return "{" + fields.stream().map(f -> f.toString()).collect(Collectors.joining("; ")) + "}";
    }
}
