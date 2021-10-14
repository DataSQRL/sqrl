package ai.dataeng.sqml.logical3;

import ai.dataeng.sqml.execution.importer.ImportSchema;
import ai.dataeng.sqml.execution.importer.ImportSchema.Mapping;
import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.TypedField;
import ai.dataeng.sqml.schema2.constraint.Constraint;
import ai.dataeng.sqml.schema2.name.Name;
import ai.dataeng.sqml.schema2.name.NameCanonicalizer;
import ai.dataeng.sqml.schema2.name.NamePath;
import ai.dataeng.sqml.tree.QualifiedName;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;

public class LogicalPlan2 {

  private final ModifiableRelationType<LogicalField> root;

  public LogicalPlan2(ModifiableRelationType<LogicalField> root) {

    this.root = root;
  }

  @Getter
  public static class Builder {
    public ModifiableRelationType<LogicalField> root = new ModifiableRelationType<>();

    public void setImportRelation(Name name, Mapping mapping,
        RelationType relationType) {
      ModifiableRelationType rel = toModifiableRelation(relationType);
      root.add(new DataField(name, rel, List.of()));
    }

    private ModifiableRelationType<LogicalField> toModifiableRelation(RelationType relationType) {
      ModifiableRelationType<LogicalField> rel = new ModifiableRelationType<>();
      for (Field field : (List<Field>)relationType.getFields()) {
        rel.add(new DataField(field.getName(), field.getType(), List.of())); //todo import field
      }

      return rel;
    }

    public Optional<ModifiableRelationType<LogicalField>> resolveModifiableRelation(QualifiedName contextName,
        QualifiedName name) {

      ModifiableRelationType<LogicalField> rel = root;
      for (String part : name.getParts()) {
        Field f = rel.getFieldByName(Name.of(part, NameCanonicalizer.SYSTEM));
        if (f == null) {
          return Optional.empty();
        }
        Type type = f.getType();
        if (!(type instanceof ModifiableRelationType)) {
          return Optional.empty();
        }
        rel = (ModifiableRelationType<LogicalField>) type;
      }

      return Optional.of(rel);
    }

    public LogicalPlan2 build() {
      return new LogicalPlan2(root);
    }
  }

  @Getter
  public static abstract class LogicalField implements TypedField {

    private final Name name;
    private final Set<Reference> references = new HashSet<>();

    protected LogicalField(Name name) {
      this.name = name;
    }

    public void addReference(@NonNull Reference reference) {
      references.add(reference);
    }
  }

  public static class DataField extends LogicalField {

    private final Type type;
    private final List<Constraint> constraints;

    public DataField(Name name, Type type, List<Constraint> constraints) {
      super(name);
      this.type = type;
      this.constraints = constraints;
    }

    @Override
    public Type getType() {
      return type;
    }
  }

  public static class DistinctRelationField extends QueryField {

    public DistinctRelationField(Name name, ModifiableRelationType<LogicalField> type) {
      super(name, type);
    }
  }

  public static abstract class QueryField extends LogicalField {

    protected final ModifiableRelationType<LogicalField> type;

    public QueryField(Name name, ModifiableRelationType<LogicalField> type) {
      super(name);
      this.type = type;
    }

    public ModifiableRelationType<LogicalField> getType() {
      return type;
    }
  }

  @Value
  public static class Reference {
    private final LogicalField from;
    private final LogicalField to;
    private final NamePath identifier;

  }
  public static class RelationshipField extends LogicalField {
    private final LogicalField parent;
    private final LogicalField to;

    //private final the-actual-join-definition from the AST plus identifier dereferencing

    public RelationshipField(Name name, LogicalField parent, LogicalField to) {
      super(name);
      this.parent = parent;
      this.to = to;
    }

    @Override
    public Type getType() {
      return to.getType();
    }
  }
  public static class SelectRelationField extends QueryField {

    private final Optional<LogicalField> parent;

    public SelectRelationField(Name name, ModifiableRelationType<LogicalField> type, Optional<LogicalField> parent) {
      super(name, type);
      this.parent = parent;
    }
  }
  public static class SourceTableField extends DataField {

    private final ImportSchema.SourceTableImport tableImport;

    public SourceTableField(Name name, ModifiableRelationType<LogicalField> type, List<Constraint> constraints,
        ImportSchema.SourceTableImport tableImport) {
      super(name, type, constraints);
      this.tableImport = tableImport;
    }

    public ModifiableRelationType<LogicalField> getType() {
      return (ModifiableRelationType<LogicalField>)super.getType();
    }
  }

  public static class SubscriptionField extends QueryField {

    public SubscriptionField(Name name, ModifiableRelationType<LogicalField> type) {
      super(name, type);
    }

  }

  public static class ModifiableRelationType<F extends Field> extends RelationType<F> {

    public ModifiableRelationType() {
      super(new ArrayList<>());
    }
    public ModifiableRelationType(List<F> fields) {
      super(fields);
    }

    public void add(F field) {
      fields.add(field);
      //Need to reset fieldsByName so this new field can be found
      fieldsByName = null;
    }

    /**
     * Returns a field with the given name or null if such does not exist.
     * If two fields have the same name, it returns the one added last (i.e. has the highest index in the array)
     *
     * @param name
     * @return
     */
    public F getFieldByName(Name name) {
      if (fieldsByName == null) {
        fieldsByName = fields.stream().collect(
            Collectors.toUnmodifiableMap(t -> t.getName(), Function.identity(),
            (v1, v2) -> v2));
      }
      return fieldsByName.get(name);
    }

  }
}
