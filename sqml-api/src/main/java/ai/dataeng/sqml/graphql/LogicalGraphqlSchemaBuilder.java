package ai.dataeng.sqml.graphql;

import static ai.dataeng.sqml.logical3.LogicalPlan.Builder.unbox;

import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.logical3.LogicalPlan;
import ai.dataeng.sqml.logical3.RelationshipField;
import ai.dataeng.sqml.schema2.ArrayType;
import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.TypedField;
import ai.dataeng.sqml.schema2.basic.BooleanType;
import ai.dataeng.sqml.schema2.basic.DateTimeType;
import ai.dataeng.sqml.schema2.basic.FloatType;
import ai.dataeng.sqml.schema2.basic.IntegerType;
import ai.dataeng.sqml.schema2.basic.NullType;
import ai.dataeng.sqml.schema2.basic.NumberType;
import ai.dataeng.sqml.schema2.basic.StringType;
import ai.dataeng.sqml.schema2.basic.UuidType;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.NodeFormatter;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.type.SqmlTypeVisitor;
import graphql.Scalars;
import graphql.schema.Coercing;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.CoercingParseValueException;
import graphql.schema.CoercingSerializeException;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import graphql.schema.idl.SchemaPrinter;
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

@Slf4j
public class LogicalGraphqlSchemaBuilder {

  public static Builder newGraphqlSchema() {
    return new Builder();
  }

  public static class Builder {
    private Analysis analysis;
    private GraphQLCodeRegistry codeRegistry;
    private Map<Class<? extends Type>, GraphQLOutputType> types = StandardScalars.getTypeMap();

    public Builder analysis(Analysis analysis) {
      this.analysis = analysis;
      return this;
    }

    public Builder additionalTypes(Map<Class<? extends Type>, GraphQLOutputType> types) {
      this.types = types;
      return this;
    }

    public Builder setCodeRegistryBuilder(GraphQLCodeRegistry codeRegistry) {
      this.codeRegistry = codeRegistry;
      return this;
    }

    public GraphQLSchema build() {
      Visitor visitor = new Visitor(analysis, types);
      visitor.visit(analysis.getPlan(), null);
      GraphQLSchema.Builder schemaBuilder = visitor.getBuilder();
      schemaBuilder.codeRegistry(this.codeRegistry);

      return schemaBuilder.build();
    }

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

  static class Visitor extends SqmlTypeVisitor<Optional<GraphQLOutputType>, Context> {
    private final Analysis analysis;
    private GraphQLSchema.Builder schemaBuilder;
    private Map<QualifiedName, GraphQLObjectType.Builder> gqlTypes = new HashMap<>();
    private Set<GraphQLType> additionalTypes = new HashSet<>();
    private GraphQLInputType bind;
    Set<Type> seen = new HashSet<>();
    public Map<Class<? extends Type>, GraphQLOutputType> typeMap;

    public Visitor(Analysis analysis, Map<Class<? extends Type>, GraphQLOutputType> typeMap) {
      this.analysis = analysis;
      this.typeMap = typeMap;
      this.schemaBuilder = GraphQLSchema.newSchema();
    }

    public GraphQLSchema.Builder getBuilder() {
      return schemaBuilder;
    }

    public Optional<GraphQLOutputType> visit(LogicalPlan logicalPlan, Context context) {
      GraphQLObjectType.Builder obj = GraphQLObjectType.newObject()
          .name("Query");

      for (TypedField field : logicalPlan.getRoot()) {
        Type type = field.getType();

        Optional<GraphQLOutputType> outputType = type.accept(this, new Context("Query", field));
        if (outputType.isPresent()) {
          String fieldName = toName(field.getName());
          GraphQLFieldDefinition f = GraphQLFieldDefinition.newFieldDefinition()
              .name(fieldName)
              .type(outputType.get())
              .build();
          obj.field(f);
        }
      }

      schemaBuilder.query(obj);
      return Optional.of(obj.build());
    }

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
      String name = toName(context.field.getName());
      if (seen.contains(relationType)) {
        return Optional.of(new GraphQLTypeReference(name));
      } else {
        seen.add(relationType);
      }
//
//      Optional<ViewTable> table = analysis.getPhysicalModel()
//          .getTableByName(QualifiedName.of(name));
//      codeRegistryBuilder.buildQuery(context.getParentType(),
//          context.getField(), table.get());

      GraphQLObjectType.Builder obj = GraphQLObjectType.newObject()
          .name(name);

      boolean hasField = false;
      for (TypedField field : (List<TypedField>)relationType.getFields()) {
        // Create any hidden types to assure they appear if referenced
        if (field.isHidden()) {
          continue;
        }

        Optional<GraphQLOutputType> type;
        if (field instanceof RelationshipField) {
          TypedField to = ((RelationshipField)field).getTo();
          if (!seen.contains(unbox(to.getType()))) { //hidden types may be referencable
            type = to.getType().accept(this, new Context(name, to));
          } else {
            String typename = toName(to.getName());
            type = Optional.of(new GraphQLTypeReference(typename));
          }
        } else {
          type = field.getType().accept(this, new Context(name, field));
        }

        if (type.isPresent()) {
          hasField = true;
          String fieldName = toName(field.getName().getDisplay());
          GraphQLFieldDefinition f = GraphQLFieldDefinition.newFieldDefinition()
              .name(fieldName)
              .type(type.get())
              .build();
          obj.field(f);
        }
      }

      if (!hasField) {
        return Optional.empty();
      }

      schemaBuilder.additionalType(obj.build());

      return Optional.of(new GraphQLTypeReference(name));
    }

    public static String toGraphqlName(String name) {
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

    public static String toName(QualifiedName name) {
      return toGraphqlName(String.join("_", name.getParts()));
    }

    public static String toName(Name name) {
      return toGraphqlName(name.getDisplay());
    }

    public static String toName(String name) {
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
}
