package ai.datasqrl.plan.global;

import ai.datasqrl.config.util.StreamUtil;
import ai.datasqrl.io.sources.dataset.TableSink;
import ai.datasqrl.physical.pipeline.ExecutionStage;
import ai.datasqrl.plan.queries.APIQuery;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Value
public class OptimizedDAG {

  /**
   * Must be in the order of the pipeline stages
   */
  List<StagePlan> stagePlans;

  public List<ReadQuery> getReadQueries() {
    return getQueriesByType(ReadQuery.class);
  }

  public List<WriteQuery> getWriteQueries() {
    return getQueriesByType(WriteQuery.class);
  }

  public<T extends Query> List<T> getQueriesByType(Class<T> clazz) {
    return StreamUtil.filterByClass(stagePlans.stream().map(StagePlan::getQueries).flatMap(List::stream),clazz)
            .collect(Collectors.toList());
  }

  @Value
  public static class StagePlan {

    @NonNull
    ExecutionStage stage;
    @NonNull
    List<? extends Query> queries;
    Collection<IndexDefinition> indexDefinitions;

  }

  public interface Query {

  }

  public interface StageSink {

    ExecutionStage getStage();

  }

  @Value
  public static class WriteQuery implements Query {

    final WriteSink sink;
    final RelNode relNode;

  }

  public interface WriteSink {

    public String getName();

  }

  @Value
  @AllArgsConstructor
  public static class DatabaseSink implements WriteSink, StageSink {

    final String nameId;
    final int numPrimaryKeys;
    final RelDataType rowType;
    final Optional<Integer> timestampIdx;
    final ExecutionStage databaseStage;

    @Override
    public String getName() {
      return getNameId();
    }

    @Override
    public ExecutionStage getStage() {
      return getDatabaseStage();
    }
  }

  @Value
  public static class ExternalSink implements WriteSink {

    String name;
    TableSink sink;

  }

  @Value
  public static class ReadQuery implements Query {

    APIQuery query;
    RelNode relNode;

  }

}
