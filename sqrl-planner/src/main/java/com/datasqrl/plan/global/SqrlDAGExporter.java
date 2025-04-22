package com.datasqrl.plan.global;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.commons.lang3.StringUtils;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.io.tables.TableType;
import com.datasqrl.plan.table.PhysicalRelationalTable;
import com.datasqrl.plan.table.ProxyImportRelationalTable;
import com.datasqrl.plan.table.PullupOperator;
import com.datasqrl.plan.table.Timestamps;
import com.datasqrl.plan.util.RelWriterWithHints;
import com.datasqrl.util.CalciteHacks;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.SuperBuilder;

/**
 * Replaced by {@link PipelineDAGExporter}
 */
@Value
@Builder
@Deprecated
public class SqrlDAGExporter {

    public static final String LINEBREAK = "\n";


    @Builder.Default
    boolean includeQueries = true;

    @Builder.Default
    boolean includeImports = true;

    @Builder.Default
    boolean withHints = false;

    @Builder.Default
    boolean includeLogicalPlan = true;

    @Builder.Default
    boolean includeSQL = true;

    //TODO: We don't yet have the physical plans in the this DAG exporter
    @Builder.Default
    boolean includePhysicalPlan = true;


    public List<Node> export(SqrlDAG dag) {
        CalciteHacks.resetToSqrlMetadataProvider();
        List<Node> result = new ArrayList<>();
        for (SqrlDAG.SqrlNode node : dag) {
            List<String> inputs = dag.getInputs(node).stream().map(SqrlDAG.SqrlNode::getId).sorted().collect(Collectors.toUnmodifiableList());
            var stage = node.getChosenStage().getEngine().getName().toLowerCase();
            if (node instanceof SqrlDAG.TableNode tableNode) {
                var table = (PhysicalRelationalTable) tableNode.getTable();
                String importInput = null;
                if (table instanceof ProxyImportRelationalTable importTable && includeImports) {
                    importInput = importTable.getBaseTable().getNameId();
                    result.add(Node.builder()
                            .id(importInput)
                            .name(importTable.getBaseTable().getTableSource().getPath().toString())
                            .type(NodeType.IMPORTS.getName())
                            .stage(stage)
                            .build());
                }
                var fields = table.getRowType().getFieldList();
                result.add(Table.builder()
                        .id(table.getNameId())
                        .name(table.getTableName().getDisplay())
                        .type(NodeType.from(table.getType()).getName())
                        .stage(stage)
                        .inputs(importInput!=null?List.of(importInput):inputs)
                        .plan(explain(table.getPlannedRelNode()))
                        .sql(toSql(table.getPlannedRelNode()))
                        .primary_key(table.getPrimaryKey().isUndefined()?null:table.getPrimaryKey().asList().stream().map(fields::get).map(RelDataTypeField::getName).collect(Collectors.toUnmodifiableList()))
                        .timestamp(table.getTimestamp().is(Timestamps.Type.UNDEFINED)?"-":fields.get(table.getTimestamp().getOnlyCandidate()).getName())
                        .schema(fields.stream().map(field -> new SchemaColumn(field.getName(), field.getType().getFullTypeString())).collect(Collectors.toUnmodifiableList()))
                        .post_processors(convert(table.getPullups(),fields))
                        .build());
            } else if (node instanceof SqrlDAG.ExportNode) {
                var export = ((SqrlDAG.ExportNode) node).getExport();
                result.add(Node.builder()
                        .id(node.getId())
                        .name(export.getSink().getPath().toString())
                        .type(NodeType.EXPORT.getName())
                        .stage(stage)
                        .inputs(inputs)
                        .build());
            } else if (node instanceof SqrlDAG.QueryNode) {
                if (!includeQueries) {
                    continue;
                }
                var query = ((SqrlDAG.QueryNode) node).getQuery();
                result.add(Query.builder()
                        .id(node.getId())
                        .name(query.getName())
                        .type(NodeType.QUERY.getName())
                        .stage(stage)
                        .plan(explain(query.getBaseQuery().getRelNode()))
                        .sql(toSql(query.getBaseQuery().getRelNode()))
                        .inputs(inputs)
                        .build());
            } else {
                throw new UnsupportedOperationException("Unexpected DAG node: " + node);
            }
        }
        return result;
    }

    private String explain(RelNode relNode) {
        if (!includeLogicalPlan) {
            return null;
        }
        CalciteHacks.resetToSqrlMetadataProvider();
        if (withHints) {
            return RelWriterWithHints.explain(relNode);
        } else {
            return relNode.explain();
        }
    }

    private String toSql(RelNode relNode) {
        if (!includeSQL) {
            return null;
        }
        return QueryPlanner.relToString(Dialect.CALCITE, relNode).getSql();
    }

    private static List<PostProcessor> convert(PullupOperator.Container pullups, List<RelDataTypeField> fields) {
        List<PostProcessor> processors = new ArrayList<>();
        if (!pullups.getNowFilter().isEmpty()) {
            var predicate = pullups.getNowFilter().getPredicate();
            Preconditions.checkArgument(predicate.isNowPredicate());
            processors.add(new PostProcessor("now-filter", fields.get(predicate.getLargerIndex()).getName() + " > now() - " + predicate.getIntervalLength() + " ms"));
        }
        if (!pullups.getTopN().isEmpty()) {
            var description = "";
            var topN = pullups.getTopN();
            if (topN.hasPartition()) {
                description += "partition="+topN.getPartition().stream().map(i -> fields.get(i).getName()).collect(Collectors.joining(", ")) + " ";
            }
            if (topN.hasLimit()) {
                description += "limit="+topN.getLimit() + " ";
            }
            if (topN.hasCollation()) {
                description += "sort="+collations2String(topN.getCollation(), fields) + " ";
            }
            if (topN.isDistinct()) {
                description += "distinct";
            }
            processors.add(new PostProcessor("topN", description));

        }
        if (!pullups.getSort().isEmpty()) {
            processors.add(new PostProcessor("sort", collations2String(pullups.getSort().getCollation(), fields)));
        }
        return processors;
    }

    private static String collations2String(RelCollation collation, List<RelDataTypeField> fields) {
        var collations = collation.getFieldCollations();
        if (collations.isEmpty()) {
            return "";
        }
        return collations.stream().map(col -> fields.get(col.getFieldIndex()) + " " + col.shortString()).collect(Collectors.joining(", "));
    }

    @AllArgsConstructor
    @Getter
    public enum NodeType {
        STREAM("stream"), STATE("state"), RELATION("relation"), QUERY("query"), EXPORT("export"), IMPORTS("import");

        private final String name;

        @Override
        public String toString() {
            return name;
        }
        public static NodeType from(TableType tableType) {
            return switch (tableType) {
            case RELATION -> RELATION;
            case STREAM -> STREAM;
            case STATE, STATIC, LOOKUP, VERSIONED_STATE -> STATE;
            default -> throw new UnsupportedOperationException("Unexpected type: " + tableType);
            };
        }
    }

    /**
     * Generic DAG node that is the base for all specific node types
     * and used for imports and exports
     */
    @Getter
    @SuperBuilder
    public static class Node implements Comparable<Node> {

        String id;
        String name;
        String type;
        String stage;
        @Builder.Default
        List<String> inputs = List.of();

        @Override
        public String toString() {
            return baseToString();
        }

        String baseToString() {
            var s = new StringBuilder();
            s.append("=== ").append(name).append(LINEBREAK);
            s.append("ID:     ").append(id).append(LINEBREAK);
            s.append("Type:   ").append(type).append(LINEBREAK);
            s.append("Stage:  ").append(stage).append(LINEBREAK);
            if (!inputs.isEmpty()) {
                s.append("Inputs: ").append(StringUtils.join(inputs, ", ")).append(LINEBREAK);
            }
            return s.toString();
        }

        @Override
        public int compareTo(SqrlDAGExporter.Node other) {
            return this.getId().compareTo(other.getId());
        }
    }

    @Getter
    @SuperBuilder
    public static class Query extends Node {

        String plan;
        String sql;

        String planToString() {
            var s = new StringBuilder();
            if (!Strings.isNullOrEmpty(plan)) {
                s.append("Plan:").append(LINEBREAK);
                s.append(plan);
//            s.append("---------------------").append(LINEBREAK);
            }
            if (!Strings.isNullOrEmpty(sql)) {
                s.append("SQL: ").append(sql);
            }
            return s.toString();
        }
        @Override
        public String toString() {
            return super.toString() + planToString();
        }
    }

    @Getter
    @SuperBuilder
    public static class Table extends Query {

        List<String> primary_key;
        String timestamp;
        List<SchemaColumn> schema;
        List<PostProcessor> post_processors;

        @Override
        public String toString() {
            var s = new StringBuilder();
            s.append(baseToString());
            s.append("Primary Key: ").append(primary_key==null?"-":StringUtils.join(primary_key,", ")).append(LINEBREAK);
            s.append("Timestamp  : ").append(timestamp).append(LINEBREAK);
            s.append("Schema:").append(LINEBREAK);
            toListString(s, schema);
            if (!post_processors.isEmpty()) {
                s.append("Post Processors:").append(LINEBREAK);
                toListString(s, post_processors);
            }
            s.append(planToString());
            return s.toString();
        }

    }

    private static void toListString(StringBuilder s, List<?> items) {
        items.forEach(i -> s.append(" - ").append(i.toString()).append(LINEBREAK));
    }

    @Value
    public static class SchemaColumn {
        String name;
        String type;

        @Override
        public String toString() {
            return name + ": " + type;
        }
    }


    @Value
    public static class PostProcessor {
        String name;
        String description;

        @Override
        public String toString() {
            return name + ": " + description;
        }
    }

}
