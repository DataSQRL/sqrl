package ai.dataeng.sqml;

import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.dag.Dag;
import ai.dataeng.sqml.tree.Assign;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.CreateSubscription;
import ai.dataeng.sqml.tree.Except;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.ExpressionAssignment;
import ai.dataeng.sqml.tree.Import;
import ai.dataeng.sqml.tree.InlineJoin;
import ai.dataeng.sqml.tree.Intersect;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QueryAssignment;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.Script;
import ai.dataeng.sqml.tree.InlineJoinBody;
import ai.dataeng.sqml.tree.Union;
import ai.dataeng.sqml.type.SqmlType;
import ai.dataeng.sqml.type.SqmlType.RelationSqmlType;
import graphql.Scalars;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
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
import java.util.logging.Logger;

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

  static class Visitor extends AstVisitor<GraphQLOutputType, Context> {
    private Logger log = Logger.getLogger(Visitor.class.getName());
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
    protected GraphQLOutputType visitNode(Node node, Context context) {
      throw new RuntimeException(String.format("Could not process node for graphql schema: %s", node));
    }

    @Override
    protected GraphQLOutputType visitScript(Script node, Context context) {
      node.getStatements().stream()
          .forEach(s->s.accept(this, context));
      return null;
    }

    @Override
    protected GraphQLOutputType visitImport(Import node, Context context) {
      return null;
    }

    @Override
    public GraphQLOutputType visitCreateSubscription(CreateSubscription node, Context context) {
      return null;
    }

    @Override
    protected GraphQLOutputType visitAssign(Assign node, Context context) {
      if (containsHiddenField(node.getName())){
        return null;
      }
      node.getRhs().accept(this, new Context(node.getName()));

      return null;
    }

    @Override
    public GraphQLOutputType visitQueryAssignment(QueryAssignment queryAssignment, Context context) {
      return queryAssignment.getQuery().accept(this, context);
    }

    @Override
    public GraphQLOutputType visitExpressionAssignment(ExpressionAssignment expressionAssignment,
        Context context) {
      GraphQLOutputType type = expressionAssignment.getExpression().accept(this, context);

      GraphQLObjectType.Builder obj = createObject(context.getName().getPrefix().orElse(context.getName()));
      GraphQLFieldDefinition f = GraphQLFieldDefinition.newFieldDefinition()
          .name(toGraphqlName(context.getName().getSuffix()))
          .type(type)
          .build();
      obj.field(f);
      return type;
    }

    public static String toGraphqlName(String name) {
      return name.replaceAll("[^A-Za-z0-9]", "");
    }

    @Override
    protected GraphQLOutputType visitQuery(Query node, Context context) {
      GraphQLOutputType type = node.getQueryBody().accept(this, context);
      return type;
    }

    @Override
    protected GraphQLOutputType visitIntersect(Intersect node, Context context) {
      return node.getRelations().get(0).accept(this, context);
    }

    @Override
    protected GraphQLOutputType visitExcept(Except node, Context context) {
      return node.getRelations().get(0).accept(this, context);
    }

    @Override
    public GraphQLOutputType visitInlineJoin(InlineJoin node, Context context) {
      QualifiedName resolvedName = getLastJoinTable(node.getJoin()); //todo walk join

      GraphQLObjectType.Builder parent = createObject(resolvedName);

      if (node.getInverse().isPresent()) {
        parent.field(
            GraphQLFieldDefinition.newFieldDefinition()
                .name(toName(QualifiedName.of(node.getInverse().get())))
                .arguments(buildRelationArguments())
                .type(GraphQLList.list(
                    GraphQLTypeReference.typeRef(toName(context.getName().getPrefix().get()))))
                .build()
        );
      }

      GraphQLTypeReference type = GraphQLTypeReference.typeRef(toName(getTableName(node.getJoin())));
      return GraphQLList.list(type);
    }

    private QualifiedName getLastJoinTable(InlineJoinBody join) {
      return join.getInlineJoinBody().map(this::getLastJoinTable).orElse(join.getTable());
    }

    @Override
    protected GraphQLOutputType visitUnion(Union node, Context context) {
      return node.getRelations().get(0).accept(this, context);
    }

    @Override
    protected GraphQLOutputType visitQuerySpecification(QuerySpecification node, Context context) {
      GraphQLObjectType.Builder builder = createObject(context.getName());

      for (Expression expression : analysis.getOutputExpressions(node)) {
        if (analysis.getName(expression) == null) {
          log.warning(String.format("Could not find api name for query expression %s", expression));
          continue;
        }
        GraphQLFieldDefinition field = GraphQLFieldDefinition.newFieldDefinition()
            .name(toGraphqlName(analysis.getName(expression)))
            .type(expression.accept(this, context))
            .build();
        builder.field(field);
      }

      if (context.getInterfaceName().isPresent()) {
        builder.withInterface(GraphQLTypeReference.typeRef(context.getInterfaceName().get()));
      }

      if (node.getLimit().isPresent() && node.getLimit().get().equalsIgnoreCase("1")) {
        return builder.build();
      }

      return GraphQLList.list(GraphQLTypeReference.typeRef(toName(context.getName())));
    }

    //Catch all for expressions
    @Override
    protected GraphQLOutputType visitExpression(Expression node,
        Context context) {
      return getType(node, context);
    }

    private GraphQLOutputType getType(Expression node, Context context) {
      Optional<SqmlType> type = analysis.getType(node);
      return type.get().accept(new GqlTypeVisitor(), context);
    }

    private QualifiedName getTableName(Relation node) {
      RelationSqmlType rel = this.analysis.getRelation(node)
          .orElseThrow();
      return rel.getRelationName();
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

    /**
     * Gets the object builder and creates the path from the base relation along the way
     */
    private GraphQLObjectType.Builder createObject(QualifiedName name) {
      GraphQLObjectType.Builder builder = gqlTypes.get(name);
      if (builder != null) {
        return builder;
      }

      String objName = toName(analysis.getName(name));

      GraphQLObjectType.Builder obj = GraphQLObjectType.newObject()
          .name(objName);
      gqlTypes.put(name, obj);

      List<String> parentName = name.getParts().subList(0, name.getParts().size() - 1);
      if (parentName.size() != 0) {
        GraphQLObjectType.Builder b = createObject(QualifiedName.of(parentName));
        b.field(GraphQLFieldDefinition.newFieldDefinition()
            .name(name.getSuffix())
            .type(GraphQLList.list(GraphQLTypeReference.typeRef(obj.build().getName())))
            .build());
      }
      return obj;
    }

    private boolean containsHiddenField(QualifiedName name) {
      for (String part : name.getParts()) {
        if (part.startsWith("_")) {
          return true;
        }
      }
      return false;
    }

    public GraphQLSchema.Builder getBuilder() {
      for (Entry<QualifiedName, GraphQLObjectType.Builder> entry : gqlTypes.entrySet()) {
        GraphQLObjectType.Builder builder = entry.getValue();
        if (builder.build().getFieldDefinitions().size() == 0) {
          log.warning(String.format("Relation has no fields: %s", entry.getValue()));
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
              .name(toName(entry.getKey()))
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
