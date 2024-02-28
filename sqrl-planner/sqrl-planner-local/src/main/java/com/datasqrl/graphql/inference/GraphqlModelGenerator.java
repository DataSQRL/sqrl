package com.datasqrl.graphql.inference;

import static com.datasqrl.graphql.generate.GraphqlSchemaUtil.hasVaryingCase;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.SerializedSqrlConfig;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.server.Model.Argument;
import com.datasqrl.graphql.server.Model.ArgumentLookupCoords;
import com.datasqrl.graphql.server.Model.ArgumentSet;
import com.datasqrl.graphql.server.Model.Coords;
import com.datasqrl.graphql.server.Model.FieldLookupCoords;
import com.datasqrl.graphql.server.Model.JdbcQuery;
import com.datasqrl.graphql.server.Model.MutationCoords;
import com.datasqrl.graphql.server.Model.PagedJdbcQuery;
import com.datasqrl.graphql.server.Model.SubscriptionCoords;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.plan.queries.APISubscription;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.google.common.base.Preconditions;
import graphql.language.FieldDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.ObjectTypeDefinition;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.validate.SqlNameMatcher;

/**
 * Returns a set of table functions that satisfy a graphql schema
 */
@Getter
public class GraphqlModelGenerator extends SchemaWalker {

  private final Map<IdentifiedQuery, QueryTemplate> databaseQueries;
  private final QueryPlanner queryPlanner;
  List<Coords> coords = new ArrayList<>();
  List<MutationCoords> mutations = new ArrayList<>();
  List<SubscriptionCoords> subscriptions = new ArrayList<>();

  public GraphqlModelGenerator(SqlNameMatcher nameMatcher, SqrlSchema schema,
      Map<IdentifiedQuery, QueryTemplate> databaseQueries, QueryPlanner queryPlanner,
      APIConnectorManager apiManager) {
    super(nameMatcher, schema, apiManager);
    this.databaseQueries = databaseQueries;
    this.queryPlanner = queryPlanner;
  }

  @Override
  protected void walkSubscription(ObjectTypeDefinition m, FieldDefinition fieldDefinition,
      TypeDefinitionRegistry registry, APISource source) {
    APISubscription apiSubscription = new APISubscription(Name.system(fieldDefinition.getName()),
        source);
    SqrlTableMacro tableFunction = schema.getTableFunction(fieldDefinition.getName());
    TableSource tableSource = apiManager.addSubscription(apiSubscription, tableFunction);

    Map<String, String> filters = new HashMap<>();
    for (InputValueDefinition input : fieldDefinition.getInputValueDefinitions()) {
      RelDataTypeField field = nameMatcher.field(tableFunction.getRowType(), input.getName());
      filters.put(input.getName(), field.getName());
    }
    SerializedSqrlConfig serialize = tableSource.getConfiguration().getConfig().serialize();
    subscriptions.add(new SubscriptionCoords(fieldDefinition.getName(), serialize, filters));
  }

  @Override
  protected void walkMutation(APISource source, TypeDefinitionRegistry registry,
      ObjectTypeDefinition m, FieldDefinition fieldDefinition) {
    TableSink tableSink = apiManager.getMutationSource(source,
        Name.system(fieldDefinition.getName()));

    SerializedSqrlConfig config = tableSink.getConfiguration().getConfig().serialize();
    mutations.add(new MutationCoords(fieldDefinition.getName(), config));
  }

  @Override
  protected void visitUnknownObject(ObjectTypeDefinition type, FieldDefinition field, NamePath path,
      Optional<RelDataType> rel) {
  }

  @Override
  protected void visitScalar(ObjectTypeDefinition type, FieldDefinition field, NamePath path,
      RelDataType relDataType, RelDataTypeField relDataTypeField) {
    //todo: walk into structured type to check all prop fetchers

    if (hasVaryingCase(field, relDataTypeField)) {
      FieldLookupCoords build = FieldLookupCoords.builder().parentType(type.getName())
          .fieldName(field.getName()).columnName(relDataTypeField.getName()).build();
      coords.add(build);
    }

  }

  @Override
  protected void visitQuery(ObjectTypeDefinition parentType, ObjectTypeDefinition toType,
      FieldDefinition field, NamePath path, Optional<RelDataType> parentRel,
      List<SqrlTableMacro> functions) {
    ArgumentLookupCoords.ArgumentLookupCoordsBuilder coordsBuilder = ArgumentLookupCoords.builder()
        .parentType(parentType.getName()).fieldName(field.getName());

    List<Entry<IdentifiedQuery, QueryTemplate>> collect = databaseQueries.entrySet().stream()
        .filter(f -> ((APIQuery) f.getKey()).getNamePath().equals(path))
        .collect(Collectors.toList());

    for (Entry<IdentifiedQuery, QueryTemplate> entry : collect) {
      JdbcQuery queryBase;
      APIQuery query = (APIQuery) entry.getKey();

      String queryStr = queryPlanner.relToString(Dialect.POSTGRES,
              queryPlanner.convertRelToDialect(Dialect.POSTGRES, entry.getValue().getRelNode()))
          .getSql();

      if (query.isLimitOffset()) {
        queryBase = new PagedJdbcQuery(queryStr, query.getParameters());
      } else {
        queryBase = new JdbcQuery(queryStr, query.getParameters());
      }

      ArgumentSet set = ArgumentSet.builder().arguments(query.getGraphqlArguments())
          .query(queryBase).build();

      coordsBuilder.match(set);
    }

    ArgumentLookupCoords build = coordsBuilder.build();
    Set<Set<Argument>> matches = build.getMatchs().stream().map(ArgumentSet::getArguments)
        .collect(Collectors.toSet());
    Preconditions.checkState(build.getMatchs().size() == matches.size());

    coords.add(build);
  }
}
