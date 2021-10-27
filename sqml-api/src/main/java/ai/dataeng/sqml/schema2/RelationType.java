package ai.dataeng.sqml.schema2;

import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import ai.dataeng.sqml.type.SqmlTypeVisitor;
import com.google.common.base.Preconditions;
import lombok.NonNull;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RelationType<F extends Field> implements Type, Iterable<F> {

    public static final RelationType EMPTY = new RelationType();

    protected final List<F> fields;

    public RelationType() {
        this(new ArrayList<>());
    }

    public RelationType(@NonNull List<F> fields) {
        //Preconditions.checkArgument(!fields.isEmpty()); TODO: should this be checked?
        this.fields = fields;
    }

    //Lazily initialized when requested because this only works for fields with names
    protected transient Map<Name,F> fieldsByName = null;

    /**
     * Returns a field with the given name or null if such does not exist.
     * If two fields have the same name, it returns the one added last (i.e. has the highest index in the array)
     *
     * @param name
     * @return
     */
    //TODO: Should be optional
    public F getFieldByName(Name name) {
        if (fieldsByName == null) {
            fieldsByName = fields.stream().collect(
                Collectors.toUnmodifiableMap(t -> t.getName(), Function.identity(),
                    (v1, v2) -> v2));
        }
        return fieldsByName.get(name);
    }

    public void add(F field) {
        fields.add(field);
        //Need to reset fieldsByName so this new field can be found
        fieldsByName = null;
    }

    @Override
    public String toString() {
        return "{" + fields.stream().map(f -> f.toString()).collect(Collectors.joining("; ")) + "}";
    }

    public static<F extends Field> Builder<F> build() {
        return new Builder<>();
    }

    public List<F> getFields() {
        return fields;
    }

    @Override
    public Iterator<F> iterator() {
        return fields.iterator();
    }

    public Optional<Field> getField(String value) {
        return Optional.ofNullable(getFieldByName(Name.of(value, NameCanonicalizer.SYSTEM)));
    }

    public RelationType withAlias(String alias) {
        RelationType rel = new RelationType();
        for (Field f : fields) {
            rel.add(f.withAlias(alias));
        }

        return rel;
    }

  public static class Builder<F extends Field> extends AbstractBuilder<F,Builder<F>> {

        public Builder() {
            super(true);
        }

        public RelationType<F> build() {
            return new RelationType<>(fields);
        }
    }

    protected static class AbstractBuilder<F extends Field, B extends AbstractBuilder<F,B>> {

        protected final List<F> fields = new ArrayList<>();
        protected final Set<Name> fieldNames;

        public AbstractBuilder(boolean checkFieldNameUniqueness) {
            if (checkFieldNameUniqueness) fieldNames = new HashSet<>();
            else fieldNames = null;
        }

        public boolean hasFieldWithName(@NonNull Name name) {
            Preconditions.checkArgument(fieldNames!=null);
            return fieldNames.contains(name);
        }

        public B add(@NonNull F field) {
            Preconditions.checkArgument(fieldNames==null || !fieldNames.contains(field.getName()));
            fields.add(field);
            if (fieldNames!=null) fieldNames.add(field.getName());
            return (B)this;
        }

        public B addAll(RelationType<F> copyFrom) {
            for (F f : copyFrom) add(f);
            return (B)this;
        }

    }

    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
        return visitor.visitRelation(this, context);
    }
}
