/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.global;

import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.util.StreamUtil;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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

  @Value
  public static class StagePlan {

    @NonNull
    ExecutionStage stage;
    @NonNull
    List<? extends Query> queries;
    Collection<IndexDefinition> indexDefinitions;

    Set<URL> jars;

    Map<String, UserDefinedFunction> udfs;

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

    final String nameId;
    final int numPrimaryKeys;
    final RelDataType rowType;
    final int timestampIdx;
    final ExecutionStage stage;

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
  public static class ReadQuery implements Query {

    APIQuery query;
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
  }
}
