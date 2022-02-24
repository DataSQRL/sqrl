package ai.dataeng.sqml.parser.processor;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.planner.RelToSql;
import ai.dataeng.sqml.planner.Relationship;
import ai.dataeng.sqml.planner.Relationship.Multiplicity;
import ai.dataeng.sqml.planner.Relationship.Type;
import ai.dataeng.sqml.planner.Table;
import ai.dataeng.sqml.planner.optimize2.SqrlLogicalTableScan;
import ai.dataeng.sqml.tree.InlineJoinBody;
import ai.dataeng.sqml.tree.JoinAssignment;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.SimpleSqrlCalciteSchema;
import org.apache.calcite.jdbc.SqrlToCalciteTableTranslator;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SqrlSchema;
import org.apache.calcite.sql.FlattenTableNames;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;

public class JoinProcessorImpl implements JoinProcessor {

  @Override
  public void process(JoinAssignment statement, Namespace namespace) {
    NamePath namePath = statement.getNamePath().getPrefix()
        .orElseThrow(()->new RuntimeException(String.format("Cannot assign join to prefix %s", statement.getNamePath())));

    Table source = namespace.lookup(namePath)
        .orElseThrow(()->new RuntimeException(String.format("Could not find source table: %s", namePath)));

    RelNode root = createPlan(statement, statement.getNamePath().getPrefix(), namespace);
    System.out.println(root);
    SqrlLogicalTableScan table = getDestinationTable(root, Optional.empty());

    System.out.println(table.getSqrlTable().getName());
    System.out.println(RelToSql.convertToSql(root));

    Table destination = table.getSqrlTable();

    source.addField(new Relationship(statement.getNamePath().getLast(),
        source, destination, Type.JOIN, getMultiplicity(root), root));

    //TODO: Inverse
    if (statement.getInlineJoin().getInverse().isPresent()) {
      Name inverseName = statement.getInlineJoin().getInverse().get();
      destination.addField(new Relationship(inverseName,
          destination, source, Type.JOIN, Multiplicity.MANY, null));
    }
  }

  private Multiplicity getMultiplicity(RelNode root) {
    if (root instanceof LogicalSort) {
      LogicalSort sort = (LogicalSort) root;
      if (sort.fetch instanceof RexLiteral) {
        RexLiteral fetch = (RexLiteral) sort.fetch;
        if (((BigDecimal)fetch.getValue()).intValue() == 1) {
          return Multiplicity.ZERO_ONE;
        }
      }
    }

    return Multiplicity.MANY;
  }

  @SneakyThrows
  public RelNode createPlan(JoinAssignment statement, Optional<NamePath> context,
      Namespace namespace) {
    SqrlToCalciteTableTranslator tableTranslator = new SqrlToCalciteTableTranslator(
        context, namespace
    );

    SimpleSqrlCalciteSchema schema = new SimpleSqrlCalciteSchema(null,
        new SqrlSchema(tableTranslator),
        "");

    Config parserConfig = SqlParser.config()
        .withParserFactory(SqlParser.config().parserFactory())
        .withLex(Lex.JAVA)
        .withConformance(SqlConformanceEnum.LENIENT)
        .withIdentifierMaxLength(256);

    FrameworkConfig config = Frameworks.newConfigBuilder()
        .defaultSchema(schema.plus())
        .parserConfig(parserConfig)
        .typeSystem(PostgresqlSqlDialect.POSTGRESQL_TYPE_SYSTEM)
        .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
        .programs(Programs.ofRules())
        .build();


    PlannerImpl planner = new PlannerImpl(config);
    SqlNode node = planner.parse(String.format("SELECT %s.* FROM _ " + statement.getQuery(),
        getLastTable(statement.getInlineJoin().getJoin()).getDisplay()));

    FlattenTableNames.rewrite(node);
    planner.validate(node);
    RelRoot root = planner.rel(node);
    return root.project();
  }

  /**
   * Reevaluates the join declaration. This function preserves shadowing of tables
   *  and their join conditions.
   */
  private RelNode reevaluateJoin() {

    return null;
  }

  private SqrlLogicalTableScan getDestinationTable(RelNode node, Optional<List<RexInputRef>> index) {
    if (node instanceof Project) {
      Project project = (Project) node;
      return getDestinationTable(project.getInput(), Optional.of(toRexInputRef(project.getProjects())));
    } else if (node instanceof TableScan) {
      return (SqrlLogicalTableScan) node;
    } else if (node instanceof LogicalJoin) {
      LogicalJoin join = (LogicalJoin) node;
      if (index.get().get(0).getIndex() == 0) {
        return getDestinationTable(join.getLeft(), index);
      } else {
        return getDestinationTable(join.getRight(), index);
      }
    } else {
      return getDestinationTable(node.getInput(0), index);
    }
  }

  private List<RexInputRef> toRexInputRef(List<RexNode> projects) {
    return projects.stream()
        .map(e->(RexInputRef) e)
        .collect(Collectors.toList());
  }

  private Name getLastTable(InlineJoinBody join) {
    if (join.getInlineJoinBody().isPresent()) {
      return getLastTable(join.getInlineJoinBody().get());
    }
    if (join.getAlias().isPresent()) {
      return Name.system(join.getAlias().get().getValue());
    }

    Preconditions.checkState(join.getTable().getLength() == 1, "Alias must be present on pathed joins");

    return join.getTable().getFirst();
  }
}
