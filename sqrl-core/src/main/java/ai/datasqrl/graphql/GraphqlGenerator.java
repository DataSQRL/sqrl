package ai.datasqrl.graphql;

import ai.datasqrl.graphql.execution.SqlClientProvider;
import ai.datasqrl.graphql.schema.GraphqlSchemaBuilder;
import ai.datasqrl.schema.Table;
import ai.datasqrl.schema.LogicalDag;
import ai.datasqrl.plan.nodes.LogicalFlinkSink;
import ai.datasqrl.plan.nodes.LogicalPgSink;
import graphql.GraphQL;
import graphql.com.google.common.collect.Maps;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.SchemaPrinter;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.table.api.TableDescriptor;

public class GraphqlGenerator {

  /**
   * Builds a graphql schema & code registry. Code registry looks at sinks to determine
   * names and tables.
   */
  public GraphQL graphql(LogicalDag dag,
      Pair<List<LogicalFlinkSink>, List<LogicalPgSink>> flinkSinks,
      Map<Table, TableDescriptor> right, SqlClientProvider postgresClient) {
    Map<Table, LogicalFlinkSink> sinks = Maps.uniqueIndex(flinkSinks.getLeft(), e->e.getSqrlTable());
    GraphQLCodeRegistry codeRegistryBuilder = new SqrlCodeRegistryBuilder()
        .build(postgresClient, sinks, right);

    GraphQLSchema graphQLSchema = GraphqlSchemaBuilder.newGraphqlSchema()
        .schema(dag.getSchema())
        .setCodeRegistryBuilder(codeRegistryBuilder)
        .build();

    System.out.println(new SchemaPrinter().print(graphQLSchema));

    GraphQL graphQL = GraphQL.newGraphQL(graphQLSchema).build();

    return graphQL;
  }

}
