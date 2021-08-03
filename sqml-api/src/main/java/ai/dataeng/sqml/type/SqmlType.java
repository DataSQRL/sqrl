package ai.dataeng.sqml.type;

import static com.google.common.base.Preconditions.checkArgument;

import ai.dataeng.sqml.analyzer.Field;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public abstract class SqmlType {
  protected final String name;

  private SqmlType(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public static abstract class ScalarSqmlType extends SqmlType {
    private ScalarSqmlType(String name) {
      super(name);
    }
  }

  public static class StringSqmlType extends ScalarSqmlType {
    public StringSqmlType() {
      super("STRING");
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visitString(this, context);
    }
  }

  public static class NumberSqmlType extends ScalarSqmlType {
    public NumberSqmlType() {
      super("NUMBER");
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visitNumber(this, context);
    }
  }

  public static class BooleanSqmlType extends ScalarSqmlType {
    public BooleanSqmlType() {
      super("BOOLEAN");
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visitBoolean(this, context);
    }
  }

  public static class ArraySqmlType extends SqmlType {

    private final SqmlType subType;

    public ArraySqmlType(SqmlType subType) {
      super("ARRAY");
      this.subType = subType;
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visitArray(this, context);
    }

    public SqmlType getSubType() {
      return subType;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ArraySqmlType that = (ArraySqmlType) o;
      return Objects.equals(subType, that.subType) && Objects.equals(name, that.getName());
    }

    @Override
    public int hashCode() {
      return Objects.hash(subType);
    }
  }

  public static class UnknownSqmlType extends SqmlType {
    public UnknownSqmlType() {
      super("UNKNOWN");
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visitUnknown(this, context);
    }
  }

  public static class DateTimeSqmlType extends ScalarSqmlType {
    public DateTimeSqmlType() {
      super("DATE");
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visitDateTime(this, context);
    }
  }
  public static class NullSqmlType extends ScalarSqmlType {
    public NullSqmlType() {
      super("NULL");
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visitNull(this, context);
    }
  }


  public static class RelationSqmlType extends SqmlType {
    private Expression expression;
    private List<Field> fields;

    public RelationSqmlType(Expression expression) {
      super("RELATION");
      this.expression = expression;
    }

    public RelationSqmlType(Field... fields) {
      super("RELATION");
      this.fields = new ArrayList<>(List.of(fields));
    }

    public RelationSqmlType(List<Field> fields) {
      super("RELATION");
      this.fields = fields;
    }

    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visitRelation(this, context);
    }

    public List<Field> getFields() {
      return fields;
    }

    public Expression getExpression() {
      return expression;
    }

    public List<Field> resolveFields(QualifiedName name) {
      //Todo: resolve scoped field: e.g. @.x
      return fields.stream()
          .filter(f->matches(f, name))
          .collect(Collectors.toList());
    }

    public Optional<Field> resolveField(QualifiedName name) {
      //todo: resolve fields via sqml logic
      return fields.stream()
          .filter(f->matches(f, name))
          .findFirst();
    }

    private boolean matches(Field f, QualifiedName name) {
      boolean matchesFieldName = f.getName().get().equalsIgnoreCase(name.getSuffix());
      if (!matchesFieldName) {
        return false;
      }
      //must match alias
      if (f.getRelationAlias().isPresent()) {
        if (name.getPrefix().isEmpty()) {
          return false;
        }

        QualifiedName alias = f.getRelationAlias().get();
        return alias.equals(name.getPrefix().get());
      }

      return false;
    }

    public void addField(Field field) {
      this.fields.add(field);
    }

    public void setField(Field field) {
      this.fields.add(field);
    }

    public RelationSqmlType join(RelationSqmlType right) {
      List<Field> joinFields = new ArrayList<>();
      joinFields.addAll(this.fields);
      joinFields.addAll(right.getFields());

      return new RelationSqmlType(joinFields);
    }

    public RelationSqmlType withAlias(String relationAlias) {

      ImmutableList.Builder<Field> fieldsBuilder = ImmutableList.builder();
      for (int i = 0; i < this.fields.size(); i++) {
        Field field = this.fields.get(i);
        Optional<String> columnAlias = field.getName();
          fieldsBuilder.add(Field.newQualified(
              QualifiedName.of(relationAlias),
              columnAlias,
              field.getType(),
              field.isHidden(),
              field.getOriginTable(),
              field.getOriginColumnName(),
              field.isAliased()));
      }

      return new RelationSqmlType(fieldsBuilder.build());
    }

  }
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SqmlType type = (SqmlType) o;
    return name.equals(type.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  public abstract <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context);
}