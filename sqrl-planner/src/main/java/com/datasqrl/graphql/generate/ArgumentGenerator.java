package com.datasqrl.graphql.generate;

import static com.datasqrl.graphql.generate.SchemaGeneratorUtil.getInputType;

import com.datasqrl.schema.Column;
import com.datasqrl.schema.Field;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.Relationship.JoinType;
import com.datasqrl.schema.SQRLTable;
import com.datasqrl.schema.SQRLTable.SqrlTableVisitor;
import com.datasqrl.schema.SqrlFieldVisitor;
import graphql.schema.GraphQLArgument;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ArgumentGenerator implements
    SqrlFieldVisitor<List<GraphQLArgument>, SchemaGeneratorContext>,
    SqrlTableVisitor<List<GraphQLArgument>, SchemaGeneratorContext> {

  @Override
  public List<GraphQLArgument> visit(Column column, SchemaGeneratorContext context) {
    throw new RuntimeException("Columns have no arguments");
  }

  @Override
  public List<GraphQLArgument> visit(Relationship field, SchemaGeneratorContext context) {
    if (!hasArguments(field)) {
      return List.of();
    }

    return field.getToTable().accept(this, context);
  }

  @Override
  public List<GraphQLArgument> visit(SQRLTable table, SchemaGeneratorContext context) {
    return table.getFields().getAccessibleFields()
        .stream().filter(SchemaGeneratorUtil::isAccessible)
        .filter(f -> f instanceof Column)
        .map(f -> GraphQLArgument.newArgument()
            .name(f.getName().getDisplay())
            .type(getInputType(((Column) f).getType()))
            .build())
        .collect(Collectors.toList());
  }

  private boolean hasArguments(Relationship field) {
    //No arguments for to-one rels or parent fields
    return field.getMultiplicity().equals(Multiplicity.MANY) &&
        !field.getJoinType().equals(JoinType.PARENT);
  }
}