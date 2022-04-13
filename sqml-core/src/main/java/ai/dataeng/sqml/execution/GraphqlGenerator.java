package ai.dataeng.sqml.execution;

import ai.dataeng.execution.SqlClientProvider;
import ai.dataeng.sqml.api.graphql.GraphqlSchemaBuilder;
import ai.dataeng.sqml.api.graphql.SqrlCodeRegistryBuilder;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.parser.sqrl.LogicalDag;
import ai.dataeng.sqml.planner.nodes.LogicalFlinkSink;
import ai.dataeng.sqml.planner.nodes.LogicalPgSink;
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
    Map<ai.dataeng.sqml.parser.Table, LogicalFlinkSink> sinks = Maps.uniqueIndex(flinkSinks.getLeft(), e->e.getSqrlTable());
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
