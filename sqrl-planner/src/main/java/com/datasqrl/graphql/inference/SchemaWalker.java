package com.datasqrl.graphql.inference;

import static com.datasqrl.graphql.server.TypeDefinitionRegistryUtil.getMutationType;
import static com.datasqrl.graphql.server.TypeDefinitionRegistryUtil.getQueryType;
import static com.datasqrl.graphql.server.TypeDefinitionRegistryUtil.getSubscriptionType;
import static com.datasqrl.graphql.util.GraphqlCheckUtil.checkState;

import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.plan.queries.APISource;
import graphql.language.FieldDefinition;
import graphql.language.ListType;
import graphql.language.NonNullType;
import graphql.language.ObjectTypeDefinition;
import graphql.language.Type;
import graphql.language.TypeDefinition;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.validate.SqlNameMatcher;

@Slf4j
public abstract class SchemaWalker {

  protected final SqlNameMatcher nameMatcher;
  protected final SqrlSchema schema;
  protected final APIConnectorManager apiManager;

  public SchemaWalker(SqlNameMatcher nameMatcher, SqrlSchema schema,
      APIConnectorManager apiManager) {
    this.nameMatcher = nameMatcher;
    this.schema = schema;
    this.apiManager = apiManager;
  }

  protected final Set<ObjectTypeDefinition> seen = new HashSet<>();

  public void walk(APISource source) {
    TypeDefinitionRegistry registry = (new SchemaParser()).parse(source.getSchemaDefinition());

    ObjectTypeDefinition queryType = getQueryType(registry);
    Optional<ObjectTypeDefinition> mutationType = getMutationType(registry);
    mutationType.ifPresent(m->walkMutation(m, registry, source));

    Optional<ObjectTypeDefinition> subscriptionType = getSubscriptionType(registry);
    subscriptionType.ifPresent(s->walkSubscription(s, registry, source));

    walk(queryType, NamePath.ROOT, Optional.empty(), registry);
  }

  private void walkSubscription(ObjectTypeDefinition m, TypeDefinitionRegistry registry,
      APISource source) {
    for(FieldDefinition fieldDefinition : m.getFieldDefinitions()) {
      walkSubscription(m, fieldDefinition, registry, source);
    }
  }

  protected abstract void walkSubscription(ObjectTypeDefinition m, FieldDefinition fieldDefinition,
      TypeDefinitionRegistry registry, APISource source);

  private void walkMutation(ObjectTypeDefinition m, TypeDefinitionRegistry registry, APISource source) {
    for(FieldDefinition fieldDefinition : m.getFieldDefinitions()) {
      walkMutation(source, registry, m, fieldDefinition);
    }
  }

  protected abstract void walkMutation(APISource source, TypeDefinitionRegistry registry,
      ObjectTypeDefinition m, FieldDefinition fieldDefinition);

  private void walk(ObjectTypeDefinition type, NamePath path, Optional<RelDataType> rel, TypeDefinitionRegistry registry) {
    if (seen.contains(type)) {
      return;
    }
    seen.add(type);
    //check to see if 'we're already resolved the type
    for (FieldDefinition field : type.getFieldDefinitions()) {
      walk(type, field, path.concat(Name.system(field.getName())), rel, registry);
    }
  }

  private void walk(ObjectTypeDefinition type, FieldDefinition field, NamePath path,
      Optional<RelDataType> rel, TypeDefinitionRegistry registry) {
    //1. Check to see if we have a table function or a reldatatype of this field.
    List<SqrlTableMacro> functions = schema.getTableFunctions(path);

    //Check to see if there exists a relationship on the schema
    if (!functions.isEmpty()) {
      visitQuery(type, field, path, rel, functions, registry);
      return;
    }

    //Check to see if it's a scalar on the relation
    if (rel.isPresent()) {
      RelDataTypeField relDataTypeField = nameMatcher.field(rel.get(), field.getName());
      if (relDataTypeField != null) {
        //If it's a row (or array of rows), walk into it
        if (relDataTypeField.getType() instanceof RelRecordType) {
          ObjectTypeDefinition type1 = registry.getType(field.getType())
              .filter(f -> f instanceof ObjectTypeDefinition).map(f -> (ObjectTypeDefinition) f)
              .orElseThrow();//assure it is a object type

          RelRecordType relRecordType = (RelRecordType) relDataTypeField.getType();
          walk(type1, path, Optional.of(relRecordType), registry);
          return;
        } else if (relDataTypeField.getType().getComponentType() != null) {
          RelDataType componentType = relDataTypeField.getType().getComponentType();

          // Unwrap the field's type to get the element type
          Type<?> fieldType = field.getType();
          fieldType = unwrapNonNullType(fieldType);

          if (fieldType instanceof ListType) {
            Type<?> elementType = ((ListType) fieldType).getType();
            elementType = unwrapNonNullType(elementType);

            if (componentType instanceof RelRecordType) {
              // The array contains records
              ObjectTypeDefinition type1 = registry.getType(elementType)
                  .filter(f -> f instanceof ObjectTypeDefinition)
                  .map(f -> (ObjectTypeDefinition) f)
                  .orElseThrow(); // Ensure it is an object type

              RelRecordType relRecordType = (RelRecordType) componentType;
              walk(type1, path, Optional.of(relRecordType), registry);
            } else {
              // The array contains scalar types
              visitScalar(type, field, path, rel.get(), relDataTypeField);
            }
          } else {
            throw new RuntimeException("Expected ListType for array field");
          }
          return;
        }

        visitScalar(type, field, path, rel.get(), relDataTypeField);
        return;
      }
    }

    visitUnknownObject(type, field, path, rel);

    //Is not a scalar or a table function, do nothing
  }
  private Type<?> unwrapNonNullType(Type<?> type) {
    if (type instanceof NonNullType) {
      return unwrapNonNullType(((NonNullType) type).getType());
    } else {
      return type;
    }
  }
  protected abstract void visitUnknownObject(ObjectTypeDefinition type, FieldDefinition field,
      NamePath path, Optional<RelDataType> rel);

  protected abstract void visitScalar(ObjectTypeDefinition type, FieldDefinition field,
      NamePath path, RelDataType relDataType, RelDataTypeField relDataTypeField);

  public Object visitQuery(ObjectTypeDefinition type, FieldDefinition field, NamePath path,
      Optional<RelDataType> rel, List<SqrlTableMacro> functions, TypeDefinitionRegistry registry) {
     Optional<TypeDefinition> optType = registry.getType(field.getType());
     checkState(optType.isPresent(), field.getType().getSourceLocation(), "Could not find object in graphql type registry");

    // Let;s check for all the types here.
    checkState(optType.get() instanceof ObjectTypeDefinition,
        type.getSourceLocation(),
        "Could not infer non-object type on graphql schema: %s", type.getName());
    if (optType.get() instanceof ObjectTypeDefinition) {

      ObjectTypeDefinition currentType = (ObjectTypeDefinition) optType.get();
      visitQuery(type, currentType, field, path, rel, functions);
      RelDataType rowType = functions.get(0).getRowType();
      // simple query, no frills
      NamePath orDefault = schema.getPathToAbsolutePathMap().getOrDefault(path, path);
      walk(currentType, orDefault, Optional.of(rowType), registry);
    } else {
      throw new RuntimeException();
      // throw, not yet supported
    }

    return null;
  }

  protected abstract void visitQuery(ObjectTypeDefinition parentType, ObjectTypeDefinition type,
      FieldDefinition field, NamePath path, Optional<RelDataType> rel,
      List<SqrlTableMacro> functions);
}
