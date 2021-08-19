package ai.dataeng.sqml.type;

import static com.google.common.base.Preconditions.checkArgument;

import ai.dataeng.sqml.analyzer.Field;
import ai.dataeng.sqml.tree.QualifiedName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public abstract class SqmlType implements Serializable {
  protected final String name;

  private SqmlType(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public SqmlType combine(SqmlType otherType) {
    if (this.equals(otherType)) return this;
    else return null;
  }

  public static abstract class ScalarSqmlType extends SqmlType {
    private ScalarSqmlType(String name) {
      super(name);
    }
  }

  public static class StringSqmlType extends ScalarSqmlType {
    public static StringSqmlType INSTANCE = new StringSqmlType();
    public StringSqmlType() {
      super("STRING");
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visitString(this, context);
    }
  }

  public static class NumberSqmlType extends ScalarSqmlType {
    public static NumberSqmlType INSTANCE = new NumberSqmlType();
    public NumberSqmlType(String name) {
      super(name);
    }
    public NumberSqmlType() {
      this("NUMBER");
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visitNumber(this, context);
    }
    public SqmlType combine(SqmlType otherType) {
      if (otherType!=null && otherType instanceof NumberSqmlType) return this;
      else return null;
    }
  }

  public static class IntegerSqmlType extends NumberSqmlType {
    public static IntegerSqmlType INSTANCE = new IntegerSqmlType();
    public IntegerSqmlType() {
      super("INTEGER");
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visitInteger(this, context);
    }
    public SqmlType combine(SqmlType otherType) {
      if (otherType!=null && otherType instanceof NumberSqmlType) return NumberSqmlType.INSTANCE;
      else return null;
    }
  }
  public static class FloatSqmlType extends NumberSqmlType {
    public static FloatSqmlType INSTANCE = new FloatSqmlType();
    public FloatSqmlType() {
      super("FLOAT");
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visitFloat(this, context);
    }
    public SqmlType combine(SqmlType otherType) {
      if (otherType!=null && otherType instanceof NumberSqmlType) return NumberSqmlType.INSTANCE;
      else return null;
    }
  }

  public static class BooleanSqmlType extends ScalarSqmlType {
    public static BooleanSqmlType INSTANCE = new BooleanSqmlType();
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
    public static UnknownSqmlType INSTANCE = new UnknownSqmlType();
    public UnknownSqmlType() {
      super("UNKNOWN");
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visitUnknown(this, context);
    }
  }

  public static class DateTimeSqmlType extends ScalarSqmlType {
    public static DateTimeSqmlType INSTANCE = new DateTimeSqmlType();
    public DateTimeSqmlType() {
      super("DATE");
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visitDateTime(this, context);
    }
  }
  public static class NullSqmlType extends ScalarSqmlType {
    public static NullSqmlType INSTANCE = new NullSqmlType();
    public NullSqmlType() {
      super("NULL");
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visitNull(this, context);
    }
  }

  public static class UuidSqmlType extends ScalarSqmlType {
    public static UuidSqmlType INSTANCE = new UuidSqmlType();
    public UuidSqmlType() {
      super("UUID");
    }
    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visitUuid(this, context);
    }
  }

  public static class RelationSqmlType extends SqmlType {
    public static RelationSqmlType INSTANCE = new RelationSqmlType();
    private List<Field> fields;
    private QualifiedName relationName;

    public RelationSqmlType() {
      super("RELATION");
      this.relationName = null;
      this.fields = new ArrayList<>();
    }

    public RelationSqmlType(QualifiedName relationName) {
      super("RELATION");
      this.fields = new ArrayList<>();
      this.relationName = relationName;
    }

    public RelationSqmlType(List<Field> fields) {
      super("RELATION");
      this.fields = new ArrayList<>(fields);
      this.relationName = null;
    }
    public RelationSqmlType(QualifiedName relationName, List<Field> fields) {
      super("RELATION");
      this.relationName = relationName;
      this.fields = new ArrayList<>(fields);
      this.relationName = null;
    }


    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
      return visitor.visitRelation(this, context);
    }

    public QualifiedName getRelationName() {
      return relationName;
    }

    public List<Field> getFields() {
      return fields;
    }

    public Optional<Field> resolveField(QualifiedName name,
        RelationSqmlType relationType) {
      List<Field> fields = resolveFields(name, relationType);
      if (fields.size() > 1) {
        throw new RuntimeException(String.format("Ambiguous fields: %s", fields));
      } else if (fields.size() == 1){
        return Optional.of(fields.get(0));
      } else {
        return Optional.empty();
      }
    }

    public List<Field> resolveFields(QualifiedName name,
        RelationSqmlType rel) {
      //if name starts with @ or has parent
      List<Field> foundFields = new ArrayList<>();
      boolean maybeAlias = name.getParts().size() > 1;
      for (Field field : rel.fields) {
        //Check for an alias
        if (maybeAlias && field.getRelationAlias().isPresent() &&
            field.getRelationAlias().get().equalsIgnoreCase(name.getParts().get(0)) &&
            field.getName().get().equalsIgnoreCase(name.getParts().get(1))) {
          //check if we have to look for more
          if (name.getParts().size() > 2) {
            Preconditions.checkState(field.getType() instanceof RelationSqmlType);
            QualifiedName nextPart = QualifiedName.of(name.getParts().subList(2, name.getParts().size()));
            RelationSqmlType nextRelation = (RelationSqmlType) field.getType();
            List<Field> nested = nextRelation.resolveFields(nextPart, nextRelation);
            foundFields.addAll(nested);
          } else {
            foundFields.add(field);
          }
        }

        //Check unaliased
        //todo fields may not have names
        if (field.getRelationAlias().isEmpty() &&
            field.getName().get().equalsIgnoreCase(name.getParts().get(0))) {
          if (name.getParts().size() > 1) {
            Preconditions.checkState(field.getType() instanceof RelationSqmlType);
            QualifiedName nextPart = QualifiedName.of(name.getParts().subList(1, name.getParts().size()));
            RelationSqmlType nextRelation = (RelationSqmlType) field.getType();
            List<Field> nested = nextRelation.resolveFields(nextPart, nextRelation);
            foundFields.addAll(nested);
          } else {
            foundFields.add(field);
          }
        }
      }

      return foundFields;
    }

    public void addField(Field field) {
      //todo shadow fields
      for (int i = fields.size() - 1; i >= 0; i--) {
        Field f = fields.get(i);
        if (f.getName().get().equalsIgnoreCase(field.getName().get())) {
          fields.remove(i);
        }
      }

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
              relationAlias,
              columnAlias,
              field.getType(),
              field.isHidden(),
              field.getOriginTable(),
              field.getOriginColumnName(),
              field.isAliased()));
      }

      return new RelationSqmlType(fieldsBuilder.build());
    }

    public List<Field> resolveFieldsWithPrefix(Optional<QualifiedName> starPrefix) {
      if (starPrefix.isEmpty()) {
        return fields;
      }
      return resolveFieldsWithPrefix(starPrefix.get(), this);
    }
    public List<Field> resolveFieldsWithPrefix(QualifiedName name, RelationSqmlType rel) {
      if (name.getParts().size() == 1 && name.getParts().get(0).equalsIgnoreCase("*")) {
        return fields;
      }

      List<Field> foundFields = new ArrayList<>();
      boolean maybeAlias = name.getParts().size() > 1;
      for (Field field : rel.fields) {
        //Check for an alias
        if (maybeAlias && field.getRelationAlias().isPresent() &&
            field.getRelationAlias().get().equalsIgnoreCase(name.getParts().get(0)) &&
            field.getName().get().equalsIgnoreCase(name.getParts().get(1))) {
          //check if we have to look for more
          if (name.getParts().size() > 2) {
            Preconditions.checkState(field.getType() instanceof RelationSqmlType);
            QualifiedName nextPart = QualifiedName.of(name.getParts().subList(2, name.getParts().size()));
            RelationSqmlType nextRelation = (RelationSqmlType) field.getType();
            List<Field> nested = nextRelation.resolveFields(nextPart, nextRelation);
            foundFields.addAll(nested);
          } else {
            foundFields.add(field);
          }
        }

        //Check unaliased
        if (field.getRelationAlias().isEmpty() &&
            field.getName().get().equalsIgnoreCase(name.getParts().get(0))) {
          if (name.getParts().size() > 1) {
            Preconditions.checkState(field.getType() instanceof RelationSqmlType);
            QualifiedName nextPart = QualifiedName.of(name.getParts().subList(1, name.getParts().size()));
            RelationSqmlType nextRelation = (RelationSqmlType) field.getType();
            List<Field> nested = nextRelation.resolveFields(nextPart, nextRelation);
            foundFields.addAll(nested);
          } else {
            foundFields.add(field);
          }
        }
      }

      return foundFields;

    }

    public void setName(QualifiedName name) {
      this.relationName = name;
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