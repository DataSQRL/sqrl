package ai.dataeng.sqml.api.graphql;

import ai.dataeng.sqml.api.graphql.GraphqlSchemaBuilder.GraphqlTypeCatalog;
import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.Field;
import ai.dataeng.sqml.planner.Relationship.Multiplicity;
import ai.dataeng.sqml.planner.Table;
import ai.dataeng.sqml.type.Type;
import ai.dataeng.sqml.type.basic.BooleanType;
import ai.dataeng.sqml.type.basic.FloatType;
import ai.dataeng.sqml.type.basic.IntegerType;
import ai.dataeng.sqml.type.basic.StringType;
import ai.dataeng.sqml.type.SqmlTypeVisitor;
import graphql.Scalars;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputObjectType.Builder;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNamedInputType;
import graphql.schema.GraphQLScalarType;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;

//Todo name mapping
//Todo: Move these builders to a better object model
//Todo: Pattern for incomplete but expandable category mapper
@AllArgsConstructor
public class GraphqlArgumentBuilder {
  Multiplicity multiplicity;
  Table table;
  boolean supportsPaging;
  GraphqlTypeCatalog catalog;

  private static final GraphQLEnumType direction = GraphQLEnumType.newEnum()
      .name("OrderDirection")
      .value("ASC")
      .value("DESC")
      .build();

  private static final GraphQLInputObjectType intFilter = buildNumericFilter("IntFilter", Scalars.GraphQLInt);
  private static final GraphQLInputObjectType floatFilter = buildNumericFilter("FloatFilter", Scalars.GraphQLFloat);
  private static final GraphQLInputObjectType stringFilter = buildEqualityFilter("StringFilter", Scalars.GraphQLString);
  private static final GraphQLInputObjectType booleanFilter = buildEqualityFilter("BooleanFilter", Scalars.GraphQLBoolean);

  public List<GraphQLArgument> build() {
    List<GraphQLArgument> arguments = new ArrayList<>();
    buildFilterArgument().map(arguments::add);
    buildOrderArgument().map(arguments::add);
    buildLimitArgument().map(arguments::add);
    buildPageSizeArgument().map(arguments::add);
    buildPageStateArgument().map(arguments::add);
    return arguments;
  }

  private Optional<GraphQLArgument> buildFilterArgument() {
    List<GraphQLInputObjectField> fields = table.getFields()
        .stream()
        .filter(c->c instanceof Column)
        .map(c->(Column) c)
        .map(this::buildFilter)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());
    if (fields.isEmpty()) {
      return Optional.empty();
    }
    GraphQLInputObjectType filter = GraphQLInputObjectType.newInputObject()
        .name(table.getName().getDisplay()+"Filter")
        .fields(fields)
        .build();

    GraphQLArgument filterArg = GraphQLArgument.newArgument()
        .name("filter")
        .type(register(filter))
        .build();
    return Optional.of(filterArg);
  }

  private Optional<GraphQLInputObjectField> buildFilter(Column column) {
    if (!column.getType().isComparable()) {
      return Optional.empty();
    }

    FilterArgumentVisitor argumentVisitor = new FilterArgumentVisitor();
    Optional<GraphQLInputObjectType> type = column.getType().accept(argumentVisitor, null);
    if (type.isEmpty()) {
      return Optional.empty();
    }
    GraphQLInputObjectField filter = GraphQLInputObjectField.newInputObjectField()
        .name(column.getName().getCanonical())
        .type(type.get())
        .build();

    return Optional.of(filter);
  }

  private Optional<GraphQLArgument> buildLimitArgument() {
    if (multiplicity == Multiplicity.MANY) {
      GraphQLArgument filter = GraphQLArgument.newArgument()
          .name("limit")
          .type(Scalars.GraphQLInt)
          .build();
      return Optional.of(filter);
    }

    return Optional.empty();
  }

  private Optional<GraphQLArgument> buildPageSizeArgument() {
    if (multiplicity == Multiplicity.MANY && supportsPaging) {
      GraphQLArgument state = GraphQLArgument.newArgument()
          .name("page_size")
          .type(Scalars.GraphQLInt)
          .build();
      return Optional.of(state);
    }
    return Optional.empty();
  }

  private Optional<GraphQLArgument> buildPageStateArgument() {
    if (multiplicity == Multiplicity.MANY && supportsPaging) {
      GraphQLArgument size = GraphQLArgument.newArgument()
          .name("page_state")
          .type(Scalars.GraphQLString)
          .build();
      return Optional.of(size);
    }
    return Optional.empty();
  }

  private Optional<GraphQLInputType> buildOrderType() {
    List<GraphQLInputObjectField> fields = new ArrayList<>();
    for (Field field : table.getFields()) {
      if (field instanceof Column) {
        Column column = (Column) field;
        if (column.getType().isOrderable()) {
          GraphQLInputObjectField orderInput = GraphQLInputObjectField.newInputObjectField()
              .name(column.getName().getDisplay())
              .type(direction)
              .build();
          fields.add(orderInput);
        }
      }
    }

    if (fields.isEmpty()) {
      return Optional.empty();
    }

    GraphQLInputObjectType.Builder order =
        fields.stream()
            .reduce(GraphQLInputObjectType.newInputObject().name(table.getName().getDisplay()+"DirectionInput"),
                    Builder::field,
                    (e,f)->f);

    return Optional.of(register(order.build()));
  }

  private GraphQLNamedInputType register(GraphQLNamedInputType type) {
    return catalog.register(type);
  }

  private Optional<GraphQLArgument> buildOrderArgument() {
    if (multiplicity == Multiplicity.MANY) {
      Optional<GraphQLInputType> orderableFields = buildOrderType();

      if (orderableFields.isPresent()) {
        return Optional.of(
            GraphQLArgument.newArgument()
              .name("order")
              .type(GraphQLList.list(orderableFields.get()))
              .build());
      }
    }
    return Optional.empty();
  }


  private static GraphQLInputObjectType buildEqualityFilter(String name,
      GraphQLScalarType scalar) {
    return GraphQLInputObjectType.newInputObject()
        .name(name)
        .field(GraphQLInputObjectField.newInputObjectField()
            .name("equals")
            .type(scalar)
            .build())
        .build();
  }

  private static GraphQLInputObjectType buildNumericFilter(String name, GraphQLScalarType scalar) {
    return GraphQLInputObjectType.newInputObject()
        .name(name)
        .field(GraphQLInputObjectField.newInputObjectField()
            .name("equals")
            .type(scalar)
            .build())
        .field(GraphQLInputObjectField.newInputObjectField()
            .name("lt")
            .type(scalar)
            .build())
        .field(GraphQLInputObjectField.newInputObjectField()
            .name("lteq")
            .type(scalar)
            .build())
        .field(GraphQLInputObjectField.newInputObjectField()
            .name("gt")
            .type(scalar)
            .build())
        .field(GraphQLInputObjectField.newInputObjectField()
            .name("gteq")
            .type(scalar)
            .build())
        .build();
  }

//    private GraphQLInputType getOrCreateBindType() {
//      if (bind == null) {
//        this.bind = GraphQLInputObjectType.newInputObject()
//            .name("bind")
//            .field(GraphQLInputObjectField.newInputObjectField()
//                .name("name")
//                .type(Scalars.GraphQLString))
//            .field(GraphQLInputObjectField.newInputObjectField()
//                .name("type")
//                .type(Scalars.GraphQLString))
//            .field(GraphQLInputObjectField.newInputObjectField()
//                .name("intType")
//                .type(Scalars.GraphQLInt)
//            ).build();
//        additionalTypes.add(bind);
//      }
//      return bind;
//    }

  class FilterArgumentVisitor extends SqmlTypeVisitor<Optional<GraphQLInputObjectType>, Void> {

    @Override
    public Optional<GraphQLInputObjectType> visitType(Type type, Void context) {
      return Optional.empty();
    }

    @Override
    public Optional<GraphQLInputObjectType> visitBooleanType(BooleanType type, Void context) {
      return Optional.of(booleanFilter);
    }

    @Override
    public Optional<GraphQLInputObjectType> visitFloatType(FloatType type, Void context) {
      return Optional.of(floatFilter);
    }

    @Override
    public Optional<GraphQLInputObjectType> visitIntegerType(IntegerType type, Void context) {
      return Optional.of(intFilter);
    }

    @Override
    public Optional<GraphQLInputObjectType> visitStringType(StringType type, Void context) {
      return Optional.of(stringFilter);
    }
  }
}
