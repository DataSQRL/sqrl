package ai.dataeng.sqml.logical;

import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.name.Name;
import ai.dataeng.sqml.schema2.name.NameCanonicalizer;
import java.util.Optional;
import lombok.Getter;

@Getter
public class DataField implements Field {

  private final Name name;
  private final Type type;
  private final Optional<String> alias;
  private final Optional<RelationDefinition> relationDefinition;

  public DataField(String name, Type type, Optional<String> alias) {
    this(Name.of(name, NameCanonicalizer.SYSTEM), type, alias);
  }
  public DataField(Name name, Type type, Optional<String> alias) {
    this(name, type, alias, Optional.empty());
  }
  public DataField(Name name, Type type, Optional<String> alias, Optional<RelationDefinition> relationDefinition) {
    this.name = name;
    this.type = type;
    this.alias = alias;
    this.relationDefinition = relationDefinition;
  }

  public static DataField newDataField(String name, Type type) {
    return new DataField(name, type, Optional.empty());
  }

  public static DataField newDataField(Name name, Type type) {
    return new DataField(name, type, Optional.empty());
  }

  public static DataField newDataField(String name, Type type, Optional<String> alias) {
    return new DataField(name, type, alias);
  }

  public static DataField newDataField(Name name, Type type, Optional<String> alias) {
    return new DataField(name, type, alias);
  }

  public static DataField newQualifiedField(Name name, Type type, Optional<String> alias,
      RelationDefinition relation) {
    return new DataField(name, type, alias, Optional.of(relation));
  }

  @Override
  public Name getName() {
    return name;
  }

  @Override
  public Type getType() {
    return Field.super.getType();
  }

  @Override
  public boolean isHidden() {
    return Field.super.isHidden();
  }
}
