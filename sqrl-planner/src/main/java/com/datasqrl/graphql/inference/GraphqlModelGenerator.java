package com.datasqrl.graphql.inference;

import static com.datasqrl.graphql.generate.GraphqlSchemaUtil.hasVaryingCase;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.TableConfig;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.engine.log.Log;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import com.datasqrl.graphql.server.RootGraphqlModel.ArgumentLookupCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.ArgumentSet;
import com.datasqrl.graphql.server.RootGraphqlModel.Coords;
import com.datasqrl.graphql.server.RootGraphqlModel.FieldLookupCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.JdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.MutationCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.PagedJdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.SubscriptionCoords;
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
  private final PhysicalPlan physicalPlan;
  List<Coords> coords = new ArrayList<>();
  List<MutationCoords> mutations = new ArrayList<>();
  List<SubscriptionCoords> subscriptions = new ArrayList<>();

  public GraphqlModelGenerator(SqlNameMatcher nameMatcher, SqrlSchema schema,
      Map<IdentifiedQuery, QueryTemplate> databaseQueries, QueryPlanner queryPlanner,
      APIConnectorManager apiManager, PhysicalPlan physicalPlan) {
    super(nameMatcher, schema, apiManager);
    this.databaseQueries = databaseQueries;
    this.queryPlanner = queryPlanner;
    this.physicalPlan = physicalPlan;
  }

  @Override
  protected void walkSubscription(ObjectTypeDefinition m, FieldDefinition fieldDefinition,
      TypeDefinitionRegistry registry, APISource source) {
    APISubscription apiSubscription = new APISubscription(Name.system(fieldDefinition.getName()),
        source);
    SqrlTableMacro tableFunction = schema.getTableFunction(fieldDefinition.getName());
    Log log = apiManager.addSubscription(apiSubscription, tableFunction);

    Map<String, String> filters = new HashMap<>();
    for (InputValueDefinition input : fieldDefinition.getInputValueDefinitions()) {
      RelDataTypeField field = nameMatcher.field(tableFunction.getRowType(), input.getName());
      filters.put(input.getName(), field.getName());
    }

    subscriptions.add(new SubscriptionCoords(fieldDefinition.getName(),
        (String)log.getConnectorContext().getMap().get("topic"), Map.of(),
        filters));
  }

  @Override
  protected void walkMutation(APISource source, TypeDefinitionRegistry registry,
      ObjectTypeDefinition m, FieldDefinition fieldDefinition) {
    TableSource tableSource = apiManager.getMutationSource(source,
        Name.system(fieldDefinition.getName()));

    Optional<TableConfig> src = schema.getImports().stream()
        .filter(t -> t.getName().equalsIgnoreCase(fieldDefinition.getName()))
        .map(t->t.getTableConfig())
        .findFirst();

    String topicName;
    if (tableSource != null) {
      Map<String, Object> map = tableSource.getConfiguration().getConnectorConfig().toMap();
      topicName = (String)map.get("topic");
    } else if (src.isPresent()){
      System.out.println();
      Map<String, Object> map = src.get().getConnectorConfig().toMap();
      topicName = (String)map.get("topic");
    } else {
      throw new RuntimeException("Could not find mutation: " + fieldDefinition.getName());
    }

    mutations.add(new MutationCoords(fieldDefinition.getName(), topicName, Map.of()));
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

    List<Entry<IdentifiedQuery, QueryTemplate>> queries = databaseQueries.entrySet().stream()
        .filter(f -> ((APIQuery) f.getKey()).getNamePath().equals(path))
        .collect(Collectors.toList());

    // No queries: use a property fetcher
    if (queries.isEmpty()) {
      return;
    }

    ArgumentLookupCoords.ArgumentLookupCoordsBuilder coordsBuilder = ArgumentLookupCoords.builder()
        .parentType(parentType.getName()).fieldName(field.getName());

    for (Entry<IdentifiedQuery, QueryTemplate> entry : queries) {
      JdbcQuery queryBase;
      APIQuery query = (APIQuery) entry.getKey();

      String queryStr = queryPlanner.relToString(Dialect.POSTGRES,
              queryPlanner.convertRelToDialect(Dialect.POSTGRES, entry.getValue().getRelNode()))
          .getSql();

      if (query.isLimitOffset()) {
        queryBase = new PagedJdbcQuery(entry.getValue().getDatabase(), queryStr, query.getParameters());
      } else {
        queryBase = new JdbcQuery(entry.getValue().getDatabase(), queryStr, query.getParameters());
      }

      ArgumentSet set = ArgumentSet.builder().arguments(query.getGraphqlArguments())
          .query(queryBase).build();

      coordsBuilder.match(set);
    }

    ArgumentLookupCoords coord = coordsBuilder.build();
    Set<Set<Argument>> matches = coord.getMatchs().stream().map(ArgumentSet::getArguments)
        .collect(Collectors.toSet());
    Preconditions.checkState(coord.getMatchs().size() == matches.size(),
        "Internal error");

    coords.add(coord);
  }
}
