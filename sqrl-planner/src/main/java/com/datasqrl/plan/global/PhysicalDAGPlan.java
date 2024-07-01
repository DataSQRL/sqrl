/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.global;

import com.datasqrl.engine.log.Log;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.datasqrl.util.StreamUtil;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.functions.UserDefinedFunction;

@Value
public class PhysicalDAGPlan {

  /**
   * Must be in the order of the pipeline stages
   */
  List<StagePlan> stagePlans;
  ExecutionPipeline pipeline;

  public List<ReadQuery> getReadQueries() {
    return getQueriesByType(ReadQuery.class);
  }

  public List<WriteQuery> getWriteQueries() {
    return getQueriesByType(WriteQuery.class);
  }

  public <T extends Query> List<T> getQueriesByType(Class<T> clazz) {
    return StreamUtil.filterByClass(
            stagePlans.stream().map(StagePlan::getQueries).flatMap(List::stream), clazz)
        .collect(Collectors.toList());
  }

  public interface StagePlan {

    ExecutionStage getStage();

    default List<? extends Query> getQueries() {
      return List.of();
    }

  }

  @Value
  public static class StreamStagePlan implements StagePlan {

    @NonNull
    ExecutionStage stage;
    @NonNull
    List<? extends Query> queries;

    Set<URL> jars;

    Map<String, UserDefinedFunction> udfs;

  }

  @Value
  public static class DatabaseStagePlan implements StagePlan {

    @NonNull
    ExecutionStage stage;
    @NonNull
    List<ReadQuery> queries;
    @NonNull
    Collection<IndexDefinition> indexDefinitions;

  }

  @Value
  public static class ServerStagePlan implements StagePlan {

    @NonNull
    ExecutionStage stage;

    List<ReadQuery> queries;
  }

  @Value
  public static class LogStagePlan implements StagePlan {

    @NonNull
    ExecutionStage stage;
    @NonNull
    List<Log> logs;

  }

  public interface Query {
    <R, C> R accept(QueryVisitor<R, C> visitor, C context);
  }

  public interface StageSink {

    ExecutionStage getStage();

  }

  @Value
  public static class WriteQuery implements Query {

    final WriteSink sink;
    final RelNode relNode;
    public <R, C> R accept(QueryVisitor<R, C> visitor, C context) {
      return visitor.accept(this, context);
    }

  }

  public interface WriteSink {

    public String getName();
    <R, C> R accept(SinkVisitor<R, C> visitor, C context);
  }

  @Value
  @AllArgsConstructor
  public static class EngineSink implements WriteSink, StageSink {

    String nameId;
    int[] primaryKeys;
    RelDataType rowType;
    OptionalInt timestampIdx;
    ExecutionStage stage;

    @Override
    public String getName() {
      return getNameId();
    }

    public <R, C> R accept(SinkVisitor<R, C> visitor, C context) {
      return visitor.accept(this, context);
    }
  }

  @Value
  public static class ExternalSink implements WriteSink {

    String name;
    TableSink tableSink;

    public ExternalSink(String name, TableSink tableSink) {
      this.name = name;
      this.tableSink = tableSink;
    }

    public <R, C> R accept(SinkVisitor<R, C> visitor, C context) {
      return visitor.accept(this, context);
    }
  }

  @Value
  @AllArgsConstructor
  public static class ExportSink implements WriteSink {
    String name;
    TableSink tableSink;
    ExecutionStage stage;

    @Override
    public <R, C> R accept(SinkVisitor<R, C> visitor, C context) {
      return visitor.accept(this, context);
    }
  }

  @Value
  public static class ReadQuery implements Query {

    IdentifiedQuery query;
    RelNode relNode;
    public <R, C> R accept(QueryVisitor<R, C> visitor, C context) {
      return visitor.accept(this, context);
    }
  }

  public interface QueryVisitor<R, C> {

    R accept(ReadQuery query, C context);
    R accept(WriteQuery writeQuery, C context);
  }

  public interface SinkVisitor<R, C> {

    R accept(ExternalSink externalSink, C context);
    R accept(EngineSink engineSink, C context);
    R accept(ExportSink exportSink, C context);
  }
}
