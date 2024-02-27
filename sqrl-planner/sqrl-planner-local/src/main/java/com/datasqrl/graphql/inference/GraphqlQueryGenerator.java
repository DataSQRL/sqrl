package com.datasqrl.graphql.inference;

import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.SerializedSqrlConfig;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.server.Model.ArgumentLookupCoords;
import com.datasqrl.graphql.server.Model.SubscriptionCoords;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.plan.queries.APISubscription;
import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.NonNullType;
import graphql.language.ObjectTypeDefinition;
import graphql.language.Value;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.validate.SqlNameMatcher;

/**
 * Returns a set of table functions that satisfy a graphql schema
 */
public class GraphqlQueryGenerator extends SchemaWalker {
  private final GraphqlQueryBuilder graphqlQueryBuilder;

  @Getter
  private final List<APIQuery> queries = new ArrayList<>();
  @Getter
  private final List<SqrlTableMacro> subscriptions = new ArrayList<>();

  public GraphqlQueryGenerator(SqlNameMatcher nameMatcher,
      SqrlSchema schema, TypeDefinitionRegistry registry,
      APISource source, GraphqlQueryBuilder graphqlQueryBuilder, APIConnectorManager apiManager) {
    super(nameMatcher, schema,  source, registry, apiManager);
    this.graphqlQueryBuilder = graphqlQueryBuilder;
  }

  @Override
  protected void walkSubscription(ObjectTypeDefinition m, FieldDefinition fieldDefinition) {
//    APISubscription apiSubscription = new APISubscription(Name.system(fieldDefinition.getName()),
//        source);
    SqrlTableMacro tableFunction = schema.getTableFunction(fieldDefinition.getName());

    subscriptions.add(tableFunction);

//
//    SerializedSqrlConfig serialize = tableSource.getConfiguration().getConfig().serialize();
//    subscriptions.add(new SubscriptionCoords(fieldDefinition.getName(), serialize, Map.of()));
  }

  @Override
  protected void walkMutation(ObjectTypeDefinition m, FieldDefinition fieldDefinition, TableSink tableSink) {
    //add mutation?
  }

  @Override
  protected void visitUnknownObject(ObjectTypeDefinition type, FieldDefinition field, NamePath path,
      Optional<RelDataType> rel) {
    System.out.println();
  }

  @Override
  protected Object visitScalar(ObjectTypeDefinition type, FieldDefinition field, NamePath path,
      RelDataType relDataType, RelDataTypeField relDataTypeField) {
    System.out.println();
    return null;
  }

  @Override
  protected ArgumentLookupCoords visitQuery(ObjectTypeDefinition parentType, ObjectTypeDefinition type,
      FieldDefinition field, NamePath path, Optional<RelDataType> parentRel,
      List<SqrlTableMacro> functions) {

    SqrlTableMacro macro = schema.getTableFunctions(path).get(0);

    List<List<ArgCombination>> argCombinations = generateCombinations(
        field.getInputValueDefinitions());

    for (List<ArgCombination> arg : argCombinations) {
      APIQuery query = graphqlQueryBuilder.create(arg, macro, parentType.getName(), field,
          parentRel.orElse(null));
      queries.add(query);
    }

    return null;
  }

  public static List<List<ArgCombination>> generateCombinations(
      List<InputValueDefinition> input) {
    List<List<ArgCombination>> result = new ArrayList<>();

    // Starting with an empty combination
    result.add(new ArrayList<>());

    for (InputValueDefinition definition : input) {
      List<List<ArgCombination>> newCombinations = new ArrayList<>();

      for (List<ArgCombination> existing : result) {

        if (definition.getDefaultValue() != null) { // A variable or the default value
          //TODO: include default value
//          List<ArgCombination> withDefault = new ArrayList<>(existing);
//          withDefault.add(new ArgCombination(definition, Optional.of(definition.getDefaultValue())));
//          newCombinations.add(withDefault);

          List<ArgCombination> withVariable = new ArrayList<>(existing);
          withVariable.add(new ArgCombination(definition, Optional.empty()));
          newCombinations.add(withVariable);
        } else if (definition.getType() instanceof NonNullType) { // Always provided by user
          existing.add(new ArgCombination(definition, Optional.empty()));
          newCombinations.add(new ArrayList<>(existing));
        } else {
          // Without current item
          newCombinations.add(new ArrayList<>(existing));

          // With current item
          existing.add(new ArgCombination(definition, Optional.empty()));
          newCombinations.add(existing);
        }
      }

      result = newCombinations;
    }

    return result;
  }

  @lombok.Value
  public static class ArgCombination {
    InputValueDefinition definition;
    Optional<Value> defaultValue;
  }
}
