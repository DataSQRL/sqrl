package com.datasqrl.v2.graphql;

import static com.datasqrl.graphql.server.TypeDefinitionRegistryUtil.getQueryType;
import static com.datasqrl.graphql.server.TypeDefinitionRegistryUtil.getSubscriptionType;
import static com.datasqrl.graphql.util.GraphqlCheckUtil.checkState;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.v2.parser.AccessModifier;
import com.datasqrl.v2.tables.SqrlTableFunction;
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
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;

/**
 * Multipurpose schema walker. It defines the actual walking methods and abstract visit methods meant to be implemented by concrete walkers.
 */
@Slf4j
public abstract class GraphqlSchemaWalker2 {

//  protected final SqlNameMatcher nameMatcher;
  protected final List<SqrlTableFunction> tableFunctions;
  protected final APIConnectorManager apiManager;

  public GraphqlSchemaWalker2(List<SqrlTableFunction> tableFunctions, APIConnectorManager apiManager) {
    this.tableFunctions = tableFunctions;
    this.apiManager = apiManager;
  }

  protected final Set<ObjectTypeDefinition> seen = new HashSet<>();

  /*
  * Schema walking methods
   */
  public void walkAPISource(APISource apiSource) {
    TypeDefinitionRegistry registry = (new SchemaParser()).parse(apiSource.getSchemaDefinition());
//TODO implement
/*
    Optional<ObjectTypeDefinition> rootMutationType = getMutationType(registry);
    rootMutationType.ifPresent(m-> walkRootMutationType(m, registry, apiSource));
*/

    Optional<ObjectTypeDefinition> rootSubscriptionTypeOpt = getSubscriptionType(registry);
    rootSubscriptionTypeOpt.ifPresent(rootSubscriptionType-> walkRootType(rootSubscriptionType, registry, apiSource));

    ObjectTypeDefinition rootQueryType = getQueryType(registry);
    walkRootType(rootQueryType, registry, apiSource); // there is always a root query type
  }


  private void walkRootMutationType(ObjectTypeDefinition type, TypeDefinitionRegistry registry, APISource source) {
    for(FieldDefinition fieldDefinition : type.getFieldDefinitions()) {
      visitMutation(type, fieldDefinition, registry, source);
    }
  }

  private void walkRootType(ObjectTypeDefinition rootType, TypeDefinitionRegistry registry, APISource apiSource) {
    for (FieldDefinition field : rootType.getFieldDefinitions()) { // fields are root table functions
      final NamePath fieldPath = NamePath.ROOT.concat(NamePath.of(field.getName()));
      final Optional<SqrlTableFunction> tableFunction = getTableFunctionFromPath(fieldPath); // root table functions are always present
      walkTableFunction(rootType, field, fieldPath, tableFunction.get(), registry, apiSource);
    }
  }

  private void walkTableFunction (ObjectTypeDefinition parentType, FieldDefinition atField, NamePath functionPath,
                                 SqrlTableFunction tableFunction, TypeDefinitionRegistry registry, APISource apiSource) {
     Optional<TypeDefinition> typeDefOpt = registry.getType(atField.getType());
     checkState(typeDefOpt.isPresent(), atField.getType().getSourceLocation(), "Could not find object in graphql type registry");
    final TypeDefinition typeDefinition = typeDefOpt.get();
    checkState(typeDefinition instanceof ObjectTypeDefinition, typeDefinition.getSourceLocation(), "Could not infer non-object type on graphql schema: %s", typeDefinition.getName());
      if (tableFunction.getVisibility().getAccess() == AccessModifier.QUERY) { // walking a query table function
        visitQuery(parentType, atField, tableFunction);
      } else { // walking a subscription table function
        visitSubscription(parentType, atField, registry, apiSource);
      }
      RelDataType functionRowType = tableFunction.getRowType();
      ObjectTypeDefinition resultType = (ObjectTypeDefinition) typeDefinition;
      walkObjectType(true, resultType, functionPath, Optional.of(functionRowType), registry, apiSource);
  }

  private void walkObjectType(boolean isFunctionResultType, ObjectTypeDefinition objectType, NamePath functionPath, Optional<RelDataType> relDataType, TypeDefinitionRegistry registry, APISource apiSource) {
    if (seen.contains(objectType)) {
      return;
    }
    seen.add(objectType);

    for (FieldDefinition field : objectType.getFieldDefinitions()) {

      NamePath fieldPath = functionPath.concat(Name.system(field.getName()));

      // Functions can have relationships, so if we are walking a function resultType, process relationship fields
      // When this method is recursively called for a nested relDataType, there can not be any relationship field
      // so in that case we call this method with isFunctionResultType == false to avoid checking for relationships
      if (isFunctionResultType) {
        final Optional<SqrlTableFunction> relationship = getTableFunctionFromPath(fieldPath);
        if (relationship.isPresent()) { // the field is a relationship field, walk the related table relationship
          walkTableFunction(objectType, field, fieldPath, relationship.get(), registry, apiSource); // there is no more nested relationships, so this method will not be recursively called
          continue;
        }
      }
      // the field is a relDataType
        RelDataTypeField relDataTypeField = relDataType.get().getField(field.getName(), true, false);
        if (relDataTypeField != null) {
          if (relDataTypeField.getType() instanceof RelRecordType) { // the field is a record
            ObjectTypeDefinition fieldType = registry.getType(field.getType())
                .filter(f -> f instanceof ObjectTypeDefinition).map(f -> (ObjectTypeDefinition) f)
                .orElseThrow();//assure it is an object type

            RelRecordType relRecordType = (RelRecordType) relDataTypeField.getType();
            walkObjectType(false, fieldType, fieldPath, Optional.of(relRecordType), registry, apiSource);
            continue;
          }
          if (relDataTypeField.getType().getComponentType() != null) { // the field is an array
            RelDataType componentType = relDataTypeField.getType().getComponentType();

            // Unwrap the nullability to get the element type
            Type<?> fieldType = field.getType();
            fieldType = unwrapNonNullType(fieldType);

            if (fieldType instanceof ListType) {
              Type<?> elementType = ((ListType) fieldType).getType();
              elementType = unwrapNonNullType(elementType);

              if (componentType instanceof RelRecordType) { // the field is an array[record]
                ObjectTypeDefinition elementObjectType = registry.getType(elementType)
                    .filter(f -> f instanceof ObjectTypeDefinition)
                    .map(f -> (ObjectTypeDefinition) f)
                    .orElseThrow(); // Ensure it is an object type

                RelRecordType relRecordType = (RelRecordType) componentType;
                walkObjectType(false, elementObjectType, fieldPath, Optional.of(relRecordType), registry, apiSource);
              } else {
                // The array contains scalar types
                visitScalar(objectType, field, fieldPath, relDataType.get(), relDataTypeField);
              }
            } else {
              throw new RuntimeException("Expected ListType for array field");
            }
            continue;
          }

          visitScalar(objectType, field, fieldPath, relDataType.get(), relDataTypeField);
          continue;
        }

      visitUnknownObject(objectType, field, fieldPath, relDataType);

      //Is not a scalar or a table function, do nothing
    }
  }

  /*
  * Abstract visit methods for concrete graphQL schema walkers to implement (for validation and graphQL model generation)
   */
  protected abstract void visitQuery(ObjectTypeDefinition parentType, FieldDefinition atField, SqrlTableFunction tableFunction);

  protected abstract void visitSubscription(ObjectTypeDefinition objectType, FieldDefinition field,
                                            TypeDefinitionRegistry registry, APISource source);

  protected abstract void visitMutation(ObjectTypeDefinition objectType, FieldDefinition field, TypeDefinitionRegistry registry,
                                        APISource source);

  protected abstract void visitUnknownObject(ObjectTypeDefinition objectType, FieldDefinition field,
                                             NamePath path, Optional<RelDataType> relDataType);

  protected abstract void visitScalar(ObjectTypeDefinition objectType, FieldDefinition field,
                                      NamePath path, RelDataType relDataType, RelDataTypeField relDataTypeField);

/*
* Utility methods
 */
private Optional<SqrlTableFunction> getTableFunctionFromPath(NamePath path) {
  final List<SqrlTableFunction> tableFunctionsAtPath = tableFunctions.stream().filter(tableFunction -> tableFunction.getFullPath().equals(path)).collect(Collectors.toList());
  assert (tableFunctionsAtPath.size() <= 1); // no overloading
  return tableFunctionsAtPath.isEmpty() ? Optional.empty() : Optional.of(tableFunctionsAtPath.get(0));
}

  private Type<?> unwrapNonNullType(Type<?> type) {
    if (type instanceof NonNullType) {
      return unwrapNonNullType(((NonNullType) type).getType());
    } else {
      return type;
    }
  }

}
