package ai.dataeng.sqml.api.graphql;

import static ai.dataeng.sqml.planner.LogicalPlanImpl.ID_DELIMITER;

import ai.dataeng.execution.DefaultDataFetcher;
import ai.dataeng.execution.SqlClientProvider;
import ai.dataeng.execution.criteria.Criteria;
import ai.dataeng.execution.criteria.EqualsCriteria;
import ai.dataeng.execution.page.NoPage;
import ai.dataeng.execution.page.SystemPageProvider;
import ai.dataeng.execution.table.H2Table;
import ai.dataeng.execution.table.TableFieldFetcher;
import ai.dataeng.execution.table.column.Columns;
import ai.dataeng.execution.table.column.DateTimeColumn;
import ai.dataeng.execution.table.column.FloatColumn;
import ai.dataeng.execution.table.column.H2Column;
import ai.dataeng.execution.table.column.IntegerColumn;
import ai.dataeng.execution.table.column.StringColumn;
import ai.dataeng.execution.table.column.UUIDColumn;
import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.Field;
import ai.dataeng.sqml.planner.Relationship;
import ai.dataeng.sqml.planner.Table;
import ai.dataeng.sqml.planner.optimize.MaterializeSource;
import ai.dataeng.sqml.tree.name.NamePath;
import ai.dataeng.sqml.type.SqmlTypeVisitor;
import ai.dataeng.sqml.type.Type;
import ai.dataeng.sqml.type.basic.DateTimeType;
import ai.dataeng.sqml.type.basic.FloatType;
import ai.dataeng.sqml.type.basic.IntegerType;
import ai.dataeng.sqml.type.basic.StringType;
import ai.dataeng.sqml.type.basic.UuidType;
import graphql.schema.FieldCoordinates;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLCodeRegistry.Builder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class SqrlCodeRegistryBuilder {

  public GraphQLCodeRegistry build(SqlClientProvider sqlClientProvider, List<MaterializeSource> sources) {
    GraphQLCodeRegistry.Builder registry = GraphQLCodeRegistry.newCodeRegistry();

    HashSet<Table> seen = new HashSet<>();
    for (MaterializeSource source : sources) {
      registerFetcher(registry, sqlClientProvider, source.getTable(), seen);
    }

    return registry.build();
  }

  private void registerFetcher(Builder registry, SqlClientProvider pool, Table table, HashSet<Table> seen) {
    if (seen.contains(table)) {
      return;
    }
    seen.add(table);

    NamePath path = table.getPath();

    if (path.getLength() == 1) {//register a special fetcher
      registry.dataFetcher(
          FieldCoordinates.coordinates("Query", table.name.getDisplay()),
          new DefaultDataFetcher(pool, new SystemPageProvider(), toTable(table, Optional.empty())));
    }

    //Look for any relationship tables and build fetchers for them
    for (Field field : table.getFields()) {
      if (field instanceof Relationship) {
        Relationship rel = (Relationship) field;
        Criteria criteria = buildCriteria(rel);

        registry.dataFetcher(
            FieldCoordinates.coordinates(getTypeName(table.getPath()), field.getName().getCanonical()),
            new DefaultDataFetcher(pool, new NoPage(), toTable(rel.getToTable(), Optional.of(criteria))));
        registerFetcher(registry, pool, rel.getToTable(), seen);
      }
    }
  }

  private Criteria buildCriteria(Relationship rel) {
    Column from = rel.table.getPrimaryKeys().get(0);
    Column to = rel.toTable.getPrimaryKeys().get(0);
    Criteria criteria = new EqualsCriteria(from.getName().getCanonical(), to.getId());
    return criteria;
  }

  private String getTypeName(NamePath path) {
    return String.join(ID_DELIMITER,
        Arrays.stream(path.getNames())
            .map(e->e.getDisplay())
            .collect(Collectors.toList()));
  }

  private TableFieldFetcher toTable(Table table, Optional<Criteria> criteria) {
    return new TableFieldFetcher(new H2Table(new Columns(toColumns(table)), table.getId()),
        criteria);
  }

  private List<H2Column> toColumns(Table table) {
    List<H2Column> list = new ArrayList<>();
    for (Field field : table.getFields()) {
      if (field instanceof Column) {
        list.add(toH2Column((Column)field));
      }
    }

    return list;
  }

  private H2Column toH2Column(Column column) {
    return column.getType().accept(new SqmlTypeVisitor<>(){
      @Override
      public H2Column visitIntegerType(IntegerType type, Column context) {
        return new IntegerColumn(column.getName().getDisplay(), column.getId());
      }

      @Override
      public H2Column visitStringType(StringType type, Column context) {
        return new StringColumn(column.getName().getDisplay(), column.getId());
      }

      @Override
      public H2Column visitUuidType(UuidType type, Column context) {
        return new UUIDColumn(column.getName().getDisplay(), column.getId());
      }

      @Override
      public H2Column visitFloatType(FloatType type, Column context) {
        return new FloatColumn(column.getName().getDisplay(), column.getId());
      }

      @Override
      public H2Column visitDateTimeType(DateTimeType type, Column context) {
        return new DateTimeColumn(column.getName().getDisplay(), column.getId());
      }

      @Override
      public H2Column visitType(Type type, Column context) {
        throw new RuntimeException("Unknown type:" + type.getClass().getName());
      }
    }, column);
  }
}
