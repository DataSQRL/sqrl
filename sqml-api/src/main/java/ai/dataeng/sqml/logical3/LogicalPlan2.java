package ai.dataeng.sqml.logical3;

import ai.dataeng.sqml.execution.importer.ImportSchema;
import ai.dataeng.sqml.schema2.ArrayType;
import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.TypedField;
import ai.dataeng.sqml.schema2.constraint.Constraint;
import ai.dataeng.sqml.schema2.name.Name;
import ai.dataeng.sqml.schema2.name.NameCanonicalizer;
import ai.dataeng.sqml.schema2.name.NamePath;
import ai.dataeng.sqml.tree.QualifiedName;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;

public class LogicalPlan2 {

  private final RelationType<LogicalField> root;

  public LogicalPlan2(RelationType<LogicalField> root) {
    this.root = root;
  }

  @Getter
  public static class Builder {
    public RelationType<LogicalField> root = new RelationType<>();

    public Optional<RelationType<LogicalField>> resolveRelation(QualifiedName contextName,
        QualifiedName name) {

      RelationType<LogicalField> rel = root;
      for (String part : name.getParts()) {
        Field f = rel.getFieldByName(Name.of(part, NameCanonicalizer.SYSTEM));
        if (f == null) {
          return Optional.empty();
        }
        Type type = unbox(f.getType());
        if (type == null) {
          return Optional.empty();
        }
        rel = (RelationType<LogicalField>) type;
      }

      return Optional.of(rel);
    }

    private Type unbox(Type type) {
      if (type instanceof RelationType) {
        return type;
      }
      if (type instanceof ArrayType) {
        ArrayType arr = (ArrayType) type;
        return unbox(arr.getSubType());
      }
      return null;
    }

    public void addRootField(LogicalField field) {
      root.add(field);
    }

    public void addField(RelationType<LogicalField> relationType, LogicalField field) {
      relationType.add(field);
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

  public static class DelegateLogicalField extends LogicalField {

    private final Field field;

    public DelegateLogicalField(Field field) {
      super(field.getName());
      this.field = field;
    }

    @Override
    public Type getType() {
      return field.getType();
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

    public DistinctRelationField(Name name, RelationType<LogicalField> type) {
      super(name, type);
    }
  }

  public static abstract class QueryField extends LogicalField {

    protected final RelationType<LogicalField> type;

    public QueryField(Name name, RelationType<LogicalField> type) {
      super(name);
      this.type = type;
    }

    public RelationType<LogicalField> getType() {
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

    public SelectRelationField(Name name, RelationType<LogicalField> type, Optional<LogicalField> parent) {
      super(name, type);
      this.parent = parent;
    }
  }
  public static class SourceTableField extends DataField {

    private final ImportSchema.SourceTableImport tableImport;

    public SourceTableField(Name name, RelationType<LogicalField> type, List<Constraint> constraints,
        ImportSchema.SourceTableImport tableImport) {
      super(name, type, constraints);
      this.tableImport = tableImport;
    }

    public RelationType<LogicalField> getType() {
      return (RelationType<LogicalField>)super.getType();
    }
  }

  public static class SubscriptionField extends QueryField {

    public SubscriptionField(Name name, RelationType<LogicalField> type) {
      super(name, type);
    }

  }
}
