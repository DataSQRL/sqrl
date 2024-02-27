package com.datasqrl.graphql.inference;

import static com.datasqrl.graphql.server.TypeDefinitionRegistryUtil.getMutationType;
import static com.datasqrl.graphql.server.TypeDefinitionRegistryUtil.getQueryType;
import static com.datasqrl.graphql.server.TypeDefinitionRegistryUtil.getSubscriptionType;
import static com.datasqrl.graphql.util.GraphqlCheckUtil.checkState;

import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.SerializedSqrlConfig;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.server.Model.ArgumentLookupCoords;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.queries.APISource;
import graphql.language.FieldDefinition;
import graphql.language.ObjectTypeDefinition;
import graphql.language.TypeDefinition;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.validate.SqlNameMatcher;

@Slf4j
@AllArgsConstructor
public abstract class SchemaWalker {

  protected final SqlNameMatcher nameMatcher;
  protected final SqrlSchema schema;
  protected final APISource source;
  protected TypeDefinitionRegistry registry;
  APIConnectorManager apiManager;
  protected final Set<ObjectTypeDefinition> seen = new HashSet<>();

  public void walk() {

    //Builds a graphql model from schema and source.

    //We walk both in lockstep, keeping careful attention to the casing of both

    //We also make room for external graphql annotations and graphql extensions

    this.registry = (new SchemaParser()).parse(source.getSchemaDefinition());

    ObjectTypeDefinition queryType = getQueryType(registry);
    Optional<ObjectTypeDefinition> mutationType = getMutationType(registry);
    mutationType.ifPresent(this::walkMutation);

    Optional<ObjectTypeDefinition> subscriptionType = getSubscriptionType(registry);
    subscriptionType.ifPresent(this::walkSubscription);

    walk(queryType, NamePath.ROOT, Optional.empty());
  }

  private void walkSubscription(ObjectTypeDefinition m) {
    for(FieldDefinition fieldDefinition : m.getFieldDefinitions()) {
      walkSubscription(m, fieldDefinition);
    }
  }

  protected abstract void walkSubscription(ObjectTypeDefinition m, FieldDefinition fieldDefinition);

  private void walkMutation(ObjectTypeDefinition m) {
    for(FieldDefinition fieldDefinition : m.getFieldDefinitions()) {
      walkMutation(m, fieldDefinition);
    }
  }

  protected abstract void walkMutation(ObjectTypeDefinition m, FieldDefinition fieldDefinition);

  private void walk(ObjectTypeDefinition type, NamePath path, Optional<RelDataType> rel) {
    if (seen.contains(type)) {
      return;
    }
    seen.add(type);
    //check to see if 'we're already resolved the type
    for (FieldDefinition field : type.getFieldDefinitions()) {
      walk(type, field, path.concat(Name.system(field.getName())), rel);
    }
  }

  private void walk(ObjectTypeDefinition type, FieldDefinition field, NamePath path,
      Optional<RelDataType> rel) {
    //1. Check to see if we have a table function or a reldatatype of this field.
    List<SqrlTableMacro> functions = schema.getTableFunctions(path);

    //Check to see if there exists a relationship on the schema
    if (!functions.isEmpty()) {
      visitQuery(type, field, path, rel, functions);
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
          walk(type1, path, Optional.of(relRecordType));
          return;
        } else if (relDataTypeField.getType().getComponentType() != null) {
          //array todo
          throw new RuntimeException();
        }

        visitScalar(type, field, path, rel.get(), relDataTypeField);
        return;
      }
    }

    visitUnknownObject(type, field, path, rel);

    //Is not a scalar or a table function, do nothing
  }

  protected abstract void visitUnknownObject(ObjectTypeDefinition type, FieldDefinition field,
      NamePath path, Optional<RelDataType> rel);

  protected abstract void visitScalar(ObjectTypeDefinition type, FieldDefinition field,
      NamePath path, RelDataType relDataType, RelDataTypeField relDataTypeField);

  public Object visitQuery(ObjectTypeDefinition type, FieldDefinition field, NamePath path,
      Optional<RelDataType> rel, List<SqrlTableMacro> functions) {
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
      walk(currentType, orDefault, Optional.of(rowType));
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
