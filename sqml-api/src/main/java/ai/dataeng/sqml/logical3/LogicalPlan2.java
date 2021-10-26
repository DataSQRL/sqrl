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
import java.util.ArrayList;
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

  public RelationType<LogicalField> getRoot() {
    return root;
  }

  @Getter
  public static class Builder {
    public RelationType<LogicalField> root = new RelationType<>();

    //First attempt to resolve field on current relation, if missing, try root scope
    public Optional<RelationType<LogicalField>> resolveRelation(QualifiedName contextName,
        QualifiedName name) {
      return resolveField(contextName, name)
          .map(e->(RelationType<LogicalField>)unbox(e.getType()));
    }
    public Optional<LogicalField> resolveField(QualifiedName contextName,
        QualifiedName name) {
      //Resolving root scope
      if (contextName.getPrefix().isEmpty()) {
        return resolveRelation(root, name);
      }

      if (name.getParts().get(0).equals("@")) {
        System.out.println();
      }

      Optional<LogicalField> contextRel = resolveRelation(root, contextName.getPrefix().get());
      if (contextRel.isEmpty()) {
        throw new RuntimeException("Cannot find parent relation");
      }
      Optional<LogicalField> localRel = resolveRelation((RelationType<LogicalField>)
          unbox(contextRel.get().getType()), name);
      if (localRel.isPresent()) {
        return localRel;
      }

      return resolveRelation(root, name);
    }

    private Optional<LogicalField> resolveRelation(RelationType<LogicalField> rel, QualifiedName name) {
      LogicalField field = null;
      List<String> parts = name.getParts();
      for (int i = 0; i < parts.size(); i++) {
        String part = parts.get(i);
        if (part.equals("@")) {
          if (i != 0) {
            return Optional.empty();
          }
          if (rel == root) {
            return Optional.empty();
          }
          continue;
        }

        LogicalField f = rel.getFieldByName(Name.of(part, NameCanonicalizer.SYSTEM));
        if (f == null) {
          return Optional.empty();
        }
        Type type = unbox(f.getType());
        if (type == null) {
          return Optional.empty();
        }
        rel = (RelationType<LogicalField>) type;
        field = f;
      }

      return Optional.of(field);
    }

    public static Type unbox(Type type) {
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

    @Override
    public boolean isHidden() {
      return name.getDisplay().startsWith("_");
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

    public LogicalField getTo() {
      return to;
    }

    @Override
    public Type getType() {
      return to.getType();
    }
  }
  public static class QueryRelationField extends QueryField {

    private final Optional<LogicalField> parent;

    public QueryRelationField(Name name, RelationType<LogicalField> type, Optional<LogicalField> parent) {
      super(name, type);
      this.parent = parent;
    }
  }

  /**
   * Field that resolve when resolving '@'
   */
  public static class SelfField extends LogicalField {

    private final RelationType self;

    public SelfField(RelationType self) {
      super(Name.of("@", NameCanonicalizer.SYSTEM));
      this.self = self;
    }

    @Override
    public Type getType() {
      return self;
    }

    @Override
    public boolean isHidden() {
      return true;
    }
  }

  /**
   * Field that resolve when resolving 'parent'
   */
  public static class ParentField extends LogicalField {

    private final RelationType self;

    public ParentField(RelationType self) {
      super(Name.of("parent", NameCanonicalizer.SYSTEM));
      this.self = self;
    }

    @Override
    public Type getType() {
      return self;
    }

    @Override
    public boolean isHidden() {
      return true;
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
