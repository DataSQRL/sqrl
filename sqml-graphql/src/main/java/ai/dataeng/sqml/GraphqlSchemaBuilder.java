package ai.dataeng.sqml;

import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.dag.Dag;
import ai.dataeng.sqml.tree.Assign;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.Except;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.ExpressionAssignment;
import ai.dataeng.sqml.tree.Intersect;
import ai.dataeng.sqml.tree.JoinSubexpression;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.QueryAssignment;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Script;
import ai.dataeng.sqml.tree.TraversalJoin;
import ai.dataeng.sqml.tree.Union;
import ai.dataeng.sqml.type.SqmlType;
import graphql.Scalars;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import graphql.schema.idl.SchemaPrinter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

public class GraphqlSchemaBuilder {

  public static Builder newGraphqlSchema() {
    return new Builder();
  }

  public static class Builder {

    private Dag dag;

    public Builder dag(Dag dag) {
      this.dag = dag;
      return this;
    }

    public GraphQLSchema build(){
      Script script = dag.getOptimizationResult().getScript();
      Analysis analysis = dag.getOptimizationResult().getAnalysis();
      Visitor visitor = new Visitor(analysis);
      script.accept(visitor, null);
      GraphQLSchema.Builder schemaBuilder = visitor.getBuilder();
      System.out.println(new SchemaPrinter().print(schemaBuilder.build()));

      return schemaBuilder.build();
    }
  }

  static class Context {

    private final QualifiedName name;
    private final Optional<String> interfaceName;

    public Context(QualifiedName name) {
      this(name, Optional.empty());
    }

    public Context(QualifiedName name, Optional<String> interfaceName) {
      this.name = name;
      this.interfaceName = interfaceName;
    }

    public Optional<String> getInterfaceName() {
      return interfaceName;
    }

    public QualifiedName getName() {
      return name;
    }

    public Context newContextWithInterface(String interfaceName) {
      return new Context(name, Optional.of(interfaceName));
    }

    public Context newContextWithAppendedName(String name) {
      List<String> n = new ArrayList<>();
      n.addAll(this.name.getParts());
      n.add(name);

      return new Context(QualifiedName.of(n), this.interfaceName);
    }
  }

  static class Visitor extends AstVisitor<Object, Context> {
    private final Analysis analysis;
    private GraphQLSchema.Builder schemaBuilder;
    private Map<QualifiedName, GraphQLObjectType.Builder> gqlTypes = new HashMap<>();
    private Set<GraphQLType> additionalTypes = new HashSet<>();
    public final GqlTypeArgumentVisitor argumentVisitor = new GqlTypeArgumentVisitor();
    GraphQLCodeRegistry.Builder codeRegistry = GraphQLCodeRegistry.newCodeRegistry();
    private GraphQLInputType bind;

    public Visitor(Analysis analysis) {
      this.analysis = analysis;
      schemaBuilder = GraphQLSchema.newSchema();
    }

    @Override
    protected Object visitScript(Script node, Context context) {
      node.getStatements().stream()
          .forEach(s->s.accept(this, context));
      return null;
    }

    @Override
    protected Object visitAssign(Assign node, Context context) {
      if (containsHiddenField(node.getName())){
        return null;
      }
      GraphQLOutputType outputType = (GraphQLOutputType) node.getRhs().accept(this, new Context(node.getName()));
      if (outputType == null) {
        throw new RuntimeException("No graphql type found for:" + node.getName());
      }

      return null;
    }

    private boolean containsHiddenField(QualifiedName name) {
      for (String part : name.getParts()) {
        if (part.startsWith("_")) {
          return true;
        }
      }
      return false;
    }

    @Override
    public GraphQLOutputType visitExpressionAssignment(ExpressionAssignment expressionAssignment,
        Context context) {
      AstVisitor that = this;

      AstVisitor<GraphQLOutputType, Void> expressionVisitor = new AstVisitor<>() {
        @Override
        public GraphQLOutputType visitJoinSubexpression(JoinSubexpression node, Void innerContext) {
          return (GraphQLOutputType)node.getJoin().accept(that, context);
        }

        @Override
        protected GraphQLOutputType visitExpression(Expression node, Void innerContext) {
          SqmlType expressionType = analysis.getType(expressionAssignment.getExpression());
          Optional<Boolean> isNonNull = analysis.isNonNull(expressionAssignment.getExpression());
          GraphQLOutputType type = asType(expressionType, isNonNull.isPresent());

          GraphQLObjectType.Builder obj = getOrCreateObjectPath(prefixOrBase(context.getName()));
          GraphQLFieldDefinition f = GraphQLFieldDefinition.newFieldDefinition()
              .name(context.getName().getSuffix())
              .type(type)
              .arguments(expressionType.accept(argumentVisitor, context))
              .build();
          obj.field(f);
          return type;
        }
      };

      return expressionAssignment.getExpression().accept(expressionVisitor, null);
    }

    @Override
    protected Object visitIntersect(Intersect node, Context context) {
      return node.getRelations().get(0).accept(this, context);
    }

    @Override
    protected Object visitExcept(Except node, Context context) {
      return node.getRelations().get(0).accept(this, context);
    }

    @Override
    public Object visitJoinSubexpression(JoinSubexpression node, Context context) {
      return node.getJoin().accept(this, context);
    }

    @Override
    public Object visitTraversalJoin(TraversalJoin node, Context context) {
      //Assure that a table type exists
      getOrCreateObjectPath(node.getTable());

      if (node.getInverse().isPresent()) {
        GraphQLObjectType.Builder parent = getOrCreateObjectPath(node.getTable());
        parent.field(
            GraphQLFieldDefinition.newFieldDefinition()
                .name(toName(node.getInverse().get().getParts()))
                .arguments(buildRelationArguments())
                .type(GraphQLList.list(
                    GraphQLTypeReference.typeRef(toName(context.getName().getPrefix().get().getParts()))))
                .build()
        );
      }

      GraphQLTypeReference type = GraphQLTypeReference.typeRef(toName(node.getTable().getParts()));
      GraphQLObjectType.Builder obj = getOrCreateObjectPath(context.getName());
      GraphQLFieldDefinition f = GraphQLFieldDefinition.newFieldDefinition()
          .name(toName(context.getName().getParts()))
          .type(type)
          .arguments(buildRelationArguments())
          .build();
      obj.field(f);
      return type;
//      if (node.getLimit().isPresent() && node.getLimit().get() == 1) {
//        return createObjRelation(ref, context.getName().getSuffix(), context);
//      } else {
//        return createObjRelation(GraphQLList.list(ref),
//            context.getName().getSuffix(), context);
//      }
    }

    @Override
    public GraphQLOutputType visitQueryAssignment(QueryAssignment queryAssignment, Context context) {
      return (GraphQLOutputType)queryAssignment.getQuery().getQueryBody().accept(this, context);
    }

    @Override
    protected Object visitUnion(Union node, Context context) {
      return node.getRelations().get(0).accept(this, context);
    }

    @Override
    protected Object visitQuerySpecification(QuerySpecification node, Context context) {
      GraphQLObjectType.Builder builder = getOrCreateObjectPath(context.getName());

      for (ResolvedField resolvedField : analysis.getFields(node)) {
        GraphQLFieldDefinition field = GraphQLFieldDefinition.newFieldDefinition()
            .name(resolvedField.getName())
            .type(asType(resolvedField.getType(), resolvedField.isNonNull()))
            .build();
        builder.field(field);
      }

      if (context.getInterfaceName().isPresent()) {
        builder.withInterface(GraphQLTypeReference.typeRef(context.getInterfaceName().get()));
      }

      if (node.getLimit().isPresent() && node.getLimit().get().equalsIgnoreCase("1")) {
        return builder.build();
      }

      return GraphQLList.list(GraphQLTypeReference.typeRef(toName(context.getName().getParts())));
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

    private String toName(List<String> parts) {
      return String.join("_", parts);
    }

    private QualifiedName prefixOrBase(QualifiedName name) {
      return name.getPrefix().isPresent() ? name.getPrefix().get() : name;
    }

    /**
     * Gets the object builder and creates the path from the base relation along the way
     */
    private GraphQLObjectType.Builder getOrCreateObjectPath(QualifiedName name) {
      GraphQLObjectType.Builder builder = gqlTypes.get(name);
      if (builder != null) {
        return builder;
      }

      String objName = toName(analysis.getName(name).getParts());

      GraphQLObjectType.Builder obj = GraphQLObjectType.newObject()
          .name(objName);
      gqlTypes.put(name, obj);

      List<String> parentName = name.getParts().subList(0, name.getParts().size() - 1);
      if (parentName.size() != 0) {
        GraphQLObjectType.Builder b = getOrCreateObjectPath(QualifiedName.of(parentName));
        b.field(GraphQLFieldDefinition.newFieldDefinition()
            .name(name.getSuffix())
            .type(GraphQLList.list(GraphQLTypeReference.typeRef(obj.build().getName())))
            .build());
      }
      return obj;
    }

    private GraphQLOutputType asType(SqmlType type, boolean nonNull) {
      GraphQLOutputType outputType = type.accept(new GqlTypeVisitor(this), null);

      return nonNull ? GraphQLNonNull.nonNull(outputType) : outputType;
    }

    public GraphQLSchema.Builder getBuilder() {
      for (Entry<QualifiedName, GraphQLObjectType.Builder> entry : gqlTypes.entrySet()) {
        GraphQLObjectType.Builder builder = entry.getValue();
        if (builder.build().getFieldDefinitions().size() == 0) {
          builder.field(GraphQLFieldDefinition.newFieldDefinition()
                  .name("stub").type(Scalars.GraphQLString).build());
        }

        schemaBuilder.additionalType(entry.getValue().build());
      }

      for (GraphQLType addl : this.additionalTypes) {
        schemaBuilder.additionalType(addl);
      }

      GraphQLObjectType.Builder query = GraphQLObjectType.newObject().name("Query");
      for (Entry<QualifiedName, GraphQLObjectType.Builder> entry : gqlTypes.entrySet()) {
        if (entry.getKey().getParts().size() == 1) {
          query.field(GraphQLFieldDefinition.newFieldDefinition()
              .name(toName(entry.getKey().getParts()))
              .arguments(buildRelationArguments())
              .type(GraphQLList.list(
                  new GraphQLTypeReference(entry.getValue().build().getName())))
              .build());
        }
      }

      schemaBuilder.query(query);

      schemaBuilder.codeRegistry(codeRegistry.build());

      return schemaBuilder;
    }
  }
}
