//package ai.dataeng.sqml.rewriter;
//
//import ai.dataeng.sqml.analyzer.Analysis;
//import ai.dataeng.sqml.analyzer.RelNode;
//import ai.dataeng.sqml.parser.SqlBaseParser.ExpressionContext;
//import ai.dataeng.sqml.parser.SqlBaseParser.ScriptContext;
//import ai.dataeng.sqml.tree.ArithmeticBinaryExpression;
//import ai.dataeng.sqml.tree.Assign;
//import ai.dataeng.sqml.tree.DefaultTraversalVisitor;
//import ai.dataeng.sqml.tree.ExpressionAssignment;
//import ai.dataeng.sqml.tree.QualifiedName;
//import ai.dataeng.sqml.tree.Relation;
//import ai.dataeng.sqml.tree.Script;
//import java.util.List;
//
//public class Rewriter {
//
//  public void rewrite(Analysis analysis) {
//      //Add Model as part of the parse tree
//      //Have a visitor that can iterate over model statements
//      //Analysis.getModel() // Has parsed model w/ expressions as leaves
//      //Transform sqml statement to sql statement
//      //
//    ScriptVisitor scriptVisitor = new ScriptVisitor(analysis);
//    script.accept(scriptVisitor, null);
//    return scriptVisitor.getViews();
//  }
//
//    class SqmlToSqlTranslator extends DefaultTraversalVisitor {
//	//1. Rewrite to add columns from statistics & types
//	//2. Then we can process each expression as if it was one data structure
//
//	//If expression, resolve column name
//
//	//Required scope:
//	// - Current relation?
//	// - Current name of sqml expression
//
//	//Script
//	// - Traverse each statement
//
//	//Expression
//	//BooleanExpression
//
//	//ValueExpression
//	//arithmeticBinary
//	// -> Convert to sql arithmetic binary
//	// ->
//	//primaryExpression
//	//Identifier
//	// -> Follow reference
//	// -> if relatively scoped: follow scope, build joins, returned scoped relation
//	// -> getOutput() -> addOther
//    }
//
//
//
//  class ScriptVisitor extends DefaultTraversalVisitor {
//    private final Analysis analysis;
//
//
//    public ScriptVisitor(Analysis analysis) {
//      this.analysis = analysis;
//    }
//
//    @Override
//    protected Object visitAssign(Assign node, Object context) {
//      QualifiedName name = node.getName();
//      SqlExpression expr = node.visit(this, new ScriptContext(name));
//
//      // If expr is a relation, Other queries need to know the relation.
//      return null;
//    }
//      class RelContext {
//	  RelNode relNode;
//	  QualifiedName name;
//      }
//    class RelVisitor extends RelationVisitor {
//	public Object visitInlineRelation(InlineExpression rel, RelContext relContext) {
//	    RelNode relNode = relContext.relNode;
//	    relNode.addInlineExpression(relContext.name, rel.getSqlExpression());
//	    return null;
//	}
//	public Object visitAggregateExpression(AggregateExpression agg, RelContext relContext) {
//	    relContext.relNode.addColumn(agg.getOutputColumns());
//	    relContext.relNode.addLeftOuterJoin(agg.getCondition(), agg.getRelationExpression());
//
//	    return null;
//	}
//    }
//    private RelNode createOrGetRelation(List<String> part) {
//      if (views.get(part) != null) {
//        return views.get(part);
//      }
//
//      RelNode relNode = new RelNode(String.join(".", part));
//
//      List<StatisticsColumn> columns = analysis.getLocalColumns(QualifiedName.of(part));
//      for (StatisticsColumn column : columns) {
//        relNode.addInlineColumn(column.getName(), column.getType());
//      }
//      views.put(part, relNode);
//      return relNode;
//    }
//
//    @Override
//    public Object visitExpressionAssignment(ExpressionAssignment expressionAssignment,
//        Object context) {
//	for (Column column : analysis.getColumnsForExpression(expressionAssignment.expression())) {
//	  rescopeColumn(column);
//	}
//
//	//1. Expression
//	// - if actually is expression
//	//2. Relation
//      return new SqlScopedExpression(expressionAssignment.getExpression().toString());
//    }
//  }
//
//  public static class SqlExpression {
//
//    private final String expressionString;
//
//    public SqlExpression(String expressionString) {
//      this.expressionString = expressionString;
//    }
//
//    public String getExpressionString() {
//      return expressionString;
//    }
//  }
//
//
//  public static class InlineFunction extends SqlExpression {
//
//    public InlineFunction(String expressionString) {
//      super(expressionString);
//    }
//  }
//
//
//  class ExpressionVisitor {
//    public void visitArithmeticBinaryExpression(ArithmeticBinaryExpression expr,
//        ExpressionContext context) {
//      Relation relation = context.getRelation();
//      QualifiedName name = context.getName();
//
//      relation.addColumn(context.getName().getPostfix(),
//          buildSqlArithmeticExpression(expr.getValueExpression(), context.getScope())
//      );
//    }
//  }
//}
