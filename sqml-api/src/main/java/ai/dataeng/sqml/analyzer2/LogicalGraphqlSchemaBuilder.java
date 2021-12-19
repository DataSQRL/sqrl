package ai.dataeng.sqml.analyzer2;

import ai.dataeng.execution.DefaultDataFetcher;
import ai.dataeng.execution.connection.JdbcPool;
import ai.dataeng.execution.page.NoPage;
import ai.dataeng.execution.page.SystemPageProvider;
import ai.dataeng.execution.table.H2Table;
import ai.dataeng.sqml.graphql.filter.JdbcArgumentBuilder;
import ai.dataeng.sqml.logical4.LogicalPlan;
import ai.dataeng.sqml.logical4.LogicalPlan.DatasetOrTable;
import ai.dataeng.sqml.logical4.LogicalPlan.Relationship;
import ai.dataeng.sqml.logical4.LogicalPlan.Relationship.Multiplicity;
import ai.dataeng.sqml.logical4.LogicalPlan.Table;
import ai.dataeng.sqml.logical4.ShadowingContainer;
import ai.dataeng.sqml.schema2.ArrayType;
import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.basic.BigIntegerType;
import ai.dataeng.sqml.schema2.basic.BooleanType;
import ai.dataeng.sqml.schema2.basic.DateTimeType;
import ai.dataeng.sqml.schema2.basic.FloatType;
import ai.dataeng.sqml.schema2.basic.IntegerType;
import ai.dataeng.sqml.schema2.basic.NullType;
import ai.dataeng.sqml.schema2.basic.NumberType;
import ai.dataeng.sqml.schema2.basic.StringType;
import ai.dataeng.sqml.schema2.basic.UuidType;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.type.SqmlTypeVisitor;
import graphql.Scalars;
import graphql.schema.Coercing;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.CoercingParseValueException;
import graphql.schema.CoercingSerializeException;
import graphql.schema.FieldCoordinates;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLCodeRegistry.Builder;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNamedInputType;
import graphql.schema.GraphQLNamedOutputType;
import graphql.schema.GraphQLNamedSchemaElement;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import io.vertx.core.Vertx;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.dataloader.DataLoaderRegistry;

@Slf4j
public class LogicalGraphqlSchemaBuilder {

  final Map<Class<? extends Type>, GraphQLOutputType> types;
  final ShadowingContainer<DatasetOrTable> schema;
  private final Vertx vertx;
  private final GraphqlTypeCatalog typeCatalog;
  GraphQLSchema.Builder schemaBuilder = GraphQLSchema.newSchema();
  UberTranslator uberTranslator;
  Map<String, H2Table> tableMap;

  public LogicalGraphqlSchemaBuilder(Map<Class<? extends Type>, GraphQLOutputType> types, ShadowingContainer<DatasetOrTable> schema,
      Vertx vertx, UberTranslator uberTranslator,Map<String, H2Table> tableMap) {

    this.types = types;
    this.schema = schema;
    this.vertx = vertx;
    this.uberTranslator = uberTranslator;
    this.tableMap = tableMap;
    this.typeCatalog = new GraphqlTypeCatalog();
  }

//  public static Builder newGraphqlSchema() {
//    return new Builder();
//  }
//
//  public static class Builder {
//    private ShadowingContainer<DatasetOrTable> schema;
//    private GraphQLCodeRegistry codeRegistry;
//    private Map<Class<? extends Type>, GraphQLOutputType> types = StandardScalars.getTypeMap();
//
//    public Builder schema(ShadowingContainer<DatasetOrTable> schema) {
//      this.schema = schema;
//      return this;
//    }
//
//    public Builder additionalTypes(Map<Class<? extends Type>, GraphQLOutputType> types) {
//      this.types = types;
//      return this;
//    }
//
//    public Builder setCodeRegistryBuilder(GraphQLCodeRegistry codeRegistry) {
//      this.codeRegistry = codeRegistry;
//      return this;
//    }
//
//    public GraphQLSchema build() {
//      LogicalGraphqlSchemaBuilder schemaBuilder = new LogicalGraphqlSchemaBuilder(types, schema);
//
//
//      return schemaBuilder.build();
//    }
//
//  }

  public GraphQLSchema build() {
    GraphQLObjectType.Builder obj = GraphQLObjectType.newObject()
        .name("Query");

    for (DatasetOrTable field : schema) {
      Table table = (Table)field;
      GraphQLFieldDefinition f = GraphQLFieldDefinition.newFieldDefinition()
          .name(table.getName().getCanonical())
          //TODO: Create a graphql object wrapper to construct this
          .type(typeCatalog.register(createPagedOutputType(table)))
          .arguments(new JdbcArgumentBuilder(Multiplicity.MANY, table, true, typeCatalog).build())
          .build();
      obj.field(f);
    }

    schemaBuilder.query(obj);
    schemaBuilder.codeRegistry(buildCodeRegistry());
    return schemaBuilder.build();
  }

  private GraphQLCodeRegistry buildCodeRegistry() {

    //In memory pool, if connection times out then the data is erased
    Pool client = JDBCPool.pool(
        vertx,
        // configure the connection
        new JDBCConnectOptions()
            .setJdbcUrl("jdbc:postgresql://localhost:5432/henneberger")
        ,
        // configure the pool
        new PoolOptions()
            .setMaxSize(1)
    );
    JdbcPool pool = new JdbcPool(client);
    DataLoaderRegistry dataLoaderRegistry = new DataLoaderRegistry();
    GraphQLCodeRegistry.Builder codeRegistry = GraphQLCodeRegistry.newCodeRegistry();
    for (DatasetOrTable ds : schema) {
      Table tbl = (Table) ds;
      String gqlName = uberTranslator.getGraphqlName(tbl);
      codeRegistry.dataFetcher(FieldCoordinates.coordinates("Query", gqlName),
          new DefaultDataFetcher(pool, new SystemPageProvider(), tableMap.get(tbl.getName().getDisplay())));
    }

    for (DatasetOrTable ds : schema) {
      Table tbl = (Table) ds;
      resolveNestedFetchers(pool, tbl, codeRegistry);
    }

    return codeRegistry.build();

  }

  private void resolveNestedFetchers(JdbcPool pool, Table tbl, Builder codeRegistry) {
    for (LogicalPlan.Field field : tbl.getFields()) {
      if (field instanceof LogicalPlan.Relationship) {
        resolveNestedFetchers(pool, (LogicalPlan.Relationship)field, codeRegistry, tbl);
      }
    }
  }

  private void resolveNestedFetchers(JdbcPool pool, Relationship field, Builder codeRegistry,
      Table parent) {
    System.out.println(uberTranslator.getGraphqlTypeName(parent) + ":" +
        uberTranslator.getGraphqlName(field.getToTable()));

    codeRegistry.dataFetcher(FieldCoordinates.coordinates(uberTranslator.getGraphqlTypeName(parent),
            uberTranslator.getGraphqlName(field.getToTable())),
        new DefaultDataFetcher(/*dataLoaderRegistry, */pool, new NoPage(), tableMap.get(field.getToTable().getName().getDisplay())));
  }

  //TODO: let page wrapper be informed by page strategy
  //TODO: Assure types are only defined once
  public GraphQLObjectType createPagedOutputType(Table table) {
    GraphQLObjectType.Builder obj = GraphQLObjectType.newObject()
        .name(table.getName().getDisplay()+"Page")
        .field(GraphQLFieldDefinition.newFieldDefinition()
            .name("data")
            .type(GraphQLList.list(typeCatalog.register(createOutputType(table))))
            .build())
        .field(GraphQLFieldDefinition.newFieldDefinition()
            .name("pageInfo")
            .type(typeCatalog.register(createPageInfo()))
            .build());

    return obj.build();
  }

  private GraphQLNamedOutputType createPageInfo() {

    return GraphQLObjectType.newObject()
        .name("PageInfo")
        .field(GraphQLFieldDefinition.newFieldDefinition()
            .name("hasNext")
            .type(Scalars.GraphQLBoolean)
            .build())
        .field(GraphQLFieldDefinition.newFieldDefinition()
            .name("cursor")
            .type(Scalars.GraphQLString)
            .build())
        .build();
  }


  public GraphQLNamedOutputType createOutputType(Table table) {
    GraphQLObjectType.Builder obj = GraphQLObjectType.newObject()
        .name(table.getName().getDisplay());

    for (LogicalPlan.Field field : table.getFields()) {
      if (!field.isVisible()) {
        continue;
      }

      GraphQLOutputType output;
      List<GraphQLArgument> argument;
      if (field instanceof LogicalPlan.Relationship) {
        LogicalPlan.Relationship rel = (LogicalPlan.Relationship) field;
        output = createOutputType(rel.getToTable());
        if (rel.getMultiplicity() == Multiplicity.MANY) {
          output = GraphQLList.list(output);
        }
        argument = new JdbcArgumentBuilder(rel.getMultiplicity(), rel.getToTable(), false, typeCatalog).build();
      } else if (field instanceof LogicalPlan.Column) {
        LogicalPlan.Column col = (LogicalPlan.Column) field;
        Visitor sqmlTypeVisitor = new Visitor(Map.of());
        output = col.getType().accept(sqmlTypeVisitor, null)
            .get();
        argument = List.of();
      } else {
        throw new RuntimeException("");
      }

      GraphQLFieldDefinition f = GraphQLFieldDefinition.newFieldDefinition()
          .name(field.getName().getCanonical())
          .type(output)
          .arguments(argument)
          .build();

      obj.field(f);
    }

    schemaBuilder.additionalType(obj.build());

    return new GraphQLTypeReference(table.getName().getDisplay());
  }

  static class StandardScalars {
    static GraphQLScalarType dateTime = GraphQLScalarType.newScalar()
        .name("DateTime").description("Built-in DateTime")
        .coercing(new Coercing<ZonedDateTime, ZonedDateTime>() {
          @Override
          public ZonedDateTime serialize(Object dataFetcherResult) throws CoercingSerializeException {
            return null;
          }

          @Override
          public ZonedDateTime parseValue(Object input) throws CoercingParseValueException {
            return null;
          }

          @Override
          public ZonedDateTime parseLiteral(Object input) throws CoercingParseLiteralException {
            return null;
          }
        }).build();

    static GraphQLScalarType uuid = GraphQLScalarType.newScalar()
        .name("Uuid").description("Built-in Uuid")
        .coercing(new Coercing<UUID, UUID>(){

          @Override
          public UUID serialize(Object dataFetcherResult) throws CoercingSerializeException {
            return parseValue(dataFetcherResult);
          }

          @Override
          public UUID parseValue(Object input) throws CoercingParseValueException {
            if (input instanceof UUID) {
              return (UUID)input;
            }
            return UUID.fromString(input.toString());
          }

          @Override
          public UUID parseLiteral(Object input) throws CoercingParseLiteralException {
            return parseValue(input);
          }
        }).build();

    static GraphQLScalarType nullType = GraphQLScalarType.newScalar()
        .name("Null").description("Built-in null")
        .coercing(new Coercing<Object, Object>(){

          @Override
          public Object serialize(Object dataFetcherResult) throws CoercingSerializeException {
            return null;
          }

          @Override
          public Object parseValue(Object input) throws CoercingParseValueException {
            return null;
          }

          @Override
          public Object parseLiteral(Object input) throws CoercingParseLiteralException {
            return null;
          }
        }).build();

    public static Map<Class<? extends Type>, GraphQLOutputType> getTypeMap() {
      Map<Class<? extends Type>, GraphQLOutputType> types = new HashMap<>();
      types.put(DateTimeType.class, dateTime);
      types.put(UuidType.class, uuid);
      types.put(NullType.class, nullType);

      return types;
    }
  }

  class Visitor extends SqmlTypeVisitor<Optional<GraphQLOutputType>, Context> {
    private GraphQLSchema.Builder schemaBuilder;
    private Map<QualifiedName, GraphQLObjectType.Builder> gqlTypes = new HashMap<>();
    private Set<GraphQLType> additionalTypes = new HashSet<>();

    private GraphQLInputType bind;
    Set<Type> seen = new HashSet<>();
    public Map<Class<? extends Type>, GraphQLOutputType> typeMap;

    public Visitor(Map<Class<? extends Type>, GraphQLOutputType> typeMap) {
      this.typeMap = typeMap;
      this.schemaBuilder = GraphQLSchema.newSchema();
    }

    public GraphQLSchema.Builder getBuilder() {
      return schemaBuilder;
    }
//
//    public Optional<GraphQLOutputType> visit(LogicalPlan logicalPlan, Context context) {
//
//    }

    @Override
    public Optional<GraphQLOutputType> visitArrayType(ArrayType type, Context context) {
      Optional<GraphQLOutputType> outputType = type.getSubType().accept(this, context);
      if (outputType.isPresent()) {
        return Optional.of(GraphQLList.list(outputType.get()));
      }
      return Optional.empty();
    }

    @Override
    public Optional<GraphQLOutputType> visitType(Type type, Context context) {
      Optional<GraphQLOutputType> outputType = Optional.ofNullable(typeMap.get(type.getClass()));

      return outputType;
    }

    @Override
    public Optional<GraphQLOutputType> visitDateTimeType(DateTimeType type, Context context) {
      return super.visitDateTimeType(type, context);
    }

    @Override
    public Optional<GraphQLOutputType> visitNullType(NullType type, Context context) {
      return super.visitNullType(type, context);
    }


    @Override
    public Optional<GraphQLOutputType> visitBigIntegerType(BigIntegerType type, Context context) {
      return Optional.of(Scalars.GraphQLInt);
    }

    public Optional<GraphQLOutputType> visitNumberType(NumberType type, Context context) {
      return Optional.of(Scalars.GraphQLFloat);
    }

    @Override
    public Optional<GraphQLOutputType> visitStringType(StringType type, Context context) {
      return Optional.of(Scalars.GraphQLString);
    }

    @Override
    public Optional<GraphQLOutputType> visitBooleanType(BooleanType type, Context context) {
      return Optional.of(Scalars.GraphQLBoolean);
    }

    @Override
    public Optional<GraphQLOutputType> visitFloatType(FloatType type, Context context) {
      return Optional.of(Scalars.GraphQLFloat);
    }

    @Override
    public Optional<GraphQLOutputType> visitIntegerType(IntegerType type, Context context) {
      return Optional.of(Scalars.GraphQLInt);
    }

    @Override
    public <F extends Field> Optional<GraphQLOutputType> visitRelation(RelationType<F> relationType,
        Context context) {
      return null;
    }

    public  String toGraphqlName(String name) {
      return name.replaceAll("[^A-Za-z0-9_]", "");
    }

    private List<GraphQLArgument> buildRelationArguments() {
      GraphQLInputType bind = getOrCreateBindType();

      GraphQLArgument filter = GraphQLArgument.newArgument().name("filter")
          .type(Scalars.GraphQLString)
          .build();

      GraphQLArgument filterBind = GraphQLArgument.newArgument().name("filterBind")
          .type(bind)
          .build();

      return List.of(filter, filterBind);
    }

    private GraphQLInputType getOrCreateBindType() {
      if (bind == null) {
        this.bind = GraphQLInputObjectType.newInputObject()
            .name("bind")
            .field(GraphQLInputObjectField.newInputObjectField()
                .name("name")
                .type(Scalars.GraphQLString))
            .field(GraphQLInputObjectField.newInputObjectField()
                .name("type")
                .type(Scalars.GraphQLString))
            .field(GraphQLInputObjectField.newInputObjectField()
                .name("intType")
                .type(Scalars.GraphQLInt)
            ).build();
        additionalTypes.add(bind);
      }
      return bind;
    }

    public String toName(QualifiedName name) {
      return toGraphqlName(String.join("_", name.getParts()));
    }

    public String toName(Name name) {
      return toGraphqlName(name.getDisplay());
    }

    public String toName(String name) {
      return toGraphqlName(name);
    }

    private boolean containsHiddenField(QualifiedName name) {
      for (String part : name.getParts()) {
        if (part.startsWith("_")) {
          return true;
        }
      }
      return false;
    }

  }

  @Value
  static class Context {
    private final String parentType;
    private final Field field;
  }

  public static class GraphqlTypeCatalog {
    Map<String, GraphQLNamedInputType> inputTypes = new HashMap<>();
    Map<String, GraphQLNamedOutputType> outputTypes = new HashMap<>();

    public GraphQLNamedOutputType register(GraphQLNamedOutputType outputType) {
      if (outputTypes.containsKey(outputType.getName())) {
        return new GraphQLTypeReference(outputType.getName());
      }
      outputTypes.put(outputType.getName(), outputType);
      return outputType;
    }
    public GraphQLNamedInputType register(GraphQLNamedInputType inputType) {
      if (inputTypes.containsKey(inputType.getName())) {
        return new GraphQLTypeReference(inputType.getName());
      }
      inputTypes.put(inputType.getName(), inputType);
      return inputType;
    }
  }
}
