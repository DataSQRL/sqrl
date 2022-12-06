package com.datasqrl.graphql.generate;

import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.conformName;
import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.getTypeReference;
import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.wrap;

import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.SQRLTable;
import com.datasqrl.schema.SQRLTable.SqrlTableVisitor;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLObjectType;
import java.util.stream.Collectors;
import org.apache.calcite.jdbc.CalciteSchemaVisitor;
import org.apache.calcite.jdbc.SqrlCalciteSchema;

/**
 * Generates the GraphQL Query type.
 */
public class QueryTypeGenerator implements
    SqrlTableVisitor<GraphQLFieldDefinition, SchemaGeneratorContext>,
    CalciteSchemaVisitor<GraphQLObjectType, SchemaGeneratorContext> {

  @Override
  public GraphQLObjectType visit(SqrlCalciteSchema schema, SchemaGeneratorContext context) {
    return GraphQLObjectType.newObject()
        .name("Query")
        .fields(schema.getRootTables().stream()
            .filter(SchemaGeneratorUtil::isAccessible)
            .map(t -> t.accept(this, context))
            .collect(Collectors.toList()))
        .build();
  }

  @Override
  public GraphQLFieldDefinition visit(SQRLTable table, SchemaGeneratorContext context) {
    return GraphQLFieldDefinition.newFieldDefinition()
        .name(conformName(table.getName().getDisplay()))
        .type(wrap(getTypeReference(table, context.getNames()), Multiplicity.MANY))
        .arguments(table.accept(new ArgumentGenerator(), context))
        .build();
  }
}