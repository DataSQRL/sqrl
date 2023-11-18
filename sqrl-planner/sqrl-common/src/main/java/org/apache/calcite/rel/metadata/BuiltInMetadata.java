//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.calcite.rel.metadata;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;
import org.apache.flink.calcite.shaded.com.google.common.collect.Multimap;

public abstract class BuiltInMetadata {
  public BuiltInMetadata() {
  }

  interface All extends Selectivity, UniqueKeys, RowCount, DistinctRowCount, PercentageOriginalRows, ColumnUniqueness, ColumnOrigin, Predicates, Collation, Distribution, Size, Parallelism, Memory, AllPredicates, ExpressionLineage, TableReferences, NodeTypes {
  }

  public interface Memory extends Metadata {
    MetadataDef<Memory> DEF = MetadataDef.of(Memory.class, Handler.class, new Method[]{BuiltInMethod.MEMORY.method, BuiltInMethod.CUMULATIVE_MEMORY_WITHIN_PHASE.method, BuiltInMethod.CUMULATIVE_MEMORY_WITHIN_PHASE_SPLIT.method});

    Double memory();

    Double cumulativeMemoryWithinPhase();

    Double cumulativeMemoryWithinPhaseSplit();

    public interface Handler extends MetadataHandler<Memory> {
      Double memory(RelNode var1, RelMetadataQuery var2);

      Double cumulativeMemoryWithinPhase(RelNode var1, RelMetadataQuery var2);

      Double cumulativeMemoryWithinPhaseSplit(RelNode var1, RelMetadataQuery var2);
    }
  }

  public interface LowerBoundCost extends Metadata {
    MetadataDef<LowerBoundCost> DEF = MetadataDef.of(LowerBoundCost.class, Handler.class, new Method[]{BuiltInMethod.LOWER_BOUND_COST.method});

    RelOptCost getLowerBoundCost(VolcanoPlanner var1);

    public interface Handler extends MetadataHandler<LowerBoundCost> {
      RelOptCost getLowerBoundCost(RelNode var1, RelMetadataQuery var2, VolcanoPlanner var3);
    }
  }

  public interface Parallelism extends Metadata {
    MetadataDef<Parallelism> DEF = MetadataDef.of(Parallelism.class, Handler.class, new Method[]{BuiltInMethod.IS_PHASE_TRANSITION.method, BuiltInMethod.SPLIT_COUNT.method});

    Boolean isPhaseTransition();

    Integer splitCount();

    public interface Handler extends MetadataHandler<Parallelism> {
      Boolean isPhaseTransition(RelNode var1, RelMetadataQuery var2);

      Integer splitCount(RelNode var1, RelMetadataQuery var2);
    }
  }

  public interface AllPredicates extends Metadata {
    MetadataDef<AllPredicates> DEF = MetadataDef.of(AllPredicates.class, Handler.class, new Method[]{BuiltInMethod.ALL_PREDICATES.method});

    RelOptPredicateList getAllPredicates();

    public interface Handler extends MetadataHandler<AllPredicates> {
      RelOptPredicateList getAllPredicates(RelNode var1, RelMetadataQuery var2);
    }
  }

  public interface Predicates extends Metadata {
    MetadataDef<Predicates> DEF = MetadataDef.of(Predicates.class, Handler.class, new Method[]{BuiltInMethod.PREDICATES.method});

    RelOptPredicateList getPredicates();

    public interface Handler extends MetadataHandler<Predicates> {
      RelOptPredicateList getPredicates(RelNode var1, RelMetadataQuery var2);
    }
  }

  public interface ExplainVisibility extends Metadata {
    MetadataDef<ExplainVisibility> DEF = MetadataDef.of(ExplainVisibility.class, Handler.class, new Method[]{BuiltInMethod.EXPLAIN_VISIBILITY.method});

    Boolean isVisibleInExplain(SqlExplainLevel var1);

    public interface Handler extends MetadataHandler<ExplainVisibility> {
      Boolean isVisibleInExplain(RelNode var1, RelMetadataQuery var2, SqlExplainLevel var3);
    }
  }

  public interface NonCumulativeCost extends Metadata {
    MetadataDef<NonCumulativeCost> DEF = MetadataDef.of(NonCumulativeCost.class, Handler.class, new Method[]{BuiltInMethod.NON_CUMULATIVE_COST.method});

    RelOptCost getNonCumulativeCost();

    public interface Handler extends MetadataHandler<NonCumulativeCost> {
      RelOptCost getNonCumulativeCost(RelNode var1, RelMetadataQuery var2);
    }
  }

  public interface CumulativeCost extends Metadata {
    MetadataDef<CumulativeCost> DEF = MetadataDef.of(CumulativeCost.class, Handler.class, new Method[]{BuiltInMethod.CUMULATIVE_COST.method});

    RelOptCost getCumulativeCost();

    public interface Handler extends MetadataHandler<CumulativeCost> {
      RelOptCost getCumulativeCost(RelNode var1, RelMetadataQuery var2);
    }
  }

  public interface TableReferences extends Metadata {
    MetadataDef<TableReferences> DEF = MetadataDef.of(TableReferences.class, Handler.class, new Method[]{BuiltInMethod.TABLE_REFERENCES.method});

    Set<RexTableInputRef.RelTableRef> getTableReferences();

    public interface Handler extends MetadataHandler<TableReferences> {
      Set<RexTableInputRef.RelTableRef> getTableReferences(RelNode var1, RelMetadataQuery var2);
    }
  }

  public interface ExpressionLineage extends Metadata {
    MetadataDef<ExpressionLineage> DEF = MetadataDef.of(ExpressionLineage.class, Handler.class, new Method[]{BuiltInMethod.EXPRESSION_LINEAGE.method});

    Set<RexNode> getExpressionLineage(RexNode var1);

    public interface Handler extends MetadataHandler<ExpressionLineage> {
      Set<RexNode> getExpressionLineage(RelNode var1, RelMetadataQuery var2, RexNode var3);
    }
  }

  public interface ColumnOrigin extends Metadata {
    MetadataDef<ColumnOrigin> DEF = MetadataDef.of(ColumnOrigin.class, Handler.class, new Method[]{BuiltInMethod.COLUMN_ORIGIN.method});

    Set<RelColumnOrigin> getColumnOrigins(int var1);

    public interface Handler extends MetadataHandler<ColumnOrigin> {
      Set<RelColumnOrigin> getColumnOrigins(RelNode var1, RelMetadataQuery var2, int var3);
    }
  }

  public interface Size extends Metadata {
    MetadataDef<Size> DEF = MetadataDef.of(Size.class, Handler.class, new Method[]{BuiltInMethod.AVERAGE_ROW_SIZE.method, BuiltInMethod.AVERAGE_COLUMN_SIZES.method});

    Double averageRowSize();

    List<Double> averageColumnSizes();

    public interface Handler extends MetadataHandler<Size> {
      Double averageRowSize(RelNode var1, RelMetadataQuery var2);

      List<Double> averageColumnSizes(RelNode var1, RelMetadataQuery var2);
    }
  }

  public interface PopulationSize extends Metadata {
    MetadataDef<PopulationSize> DEF = MetadataDef.of(PopulationSize.class, Handler.class, new Method[]{BuiltInMethod.POPULATION_SIZE.method});

    Double getPopulationSize(ImmutableBitSet var1);

    public interface Handler extends MetadataHandler<PopulationSize> {
      Double getPopulationSize(RelNode var1, RelMetadataQuery var2, ImmutableBitSet var3);
    }
  }

  public interface PercentageOriginalRows extends Metadata {
    MetadataDef<PercentageOriginalRows> DEF = MetadataDef.of(PercentageOriginalRows.class, Handler.class, new Method[]{BuiltInMethod.PERCENTAGE_ORIGINAL_ROWS.method});

    Double getPercentageOriginalRows();

    public interface Handler extends MetadataHandler<PercentageOriginalRows> {
      Double getPercentageOriginalRows(RelNode var1, RelMetadataQuery var2);
    }
  }

  public interface DistinctRowCount extends Metadata {
    MetadataDef<DistinctRowCount> DEF = MetadataDef.of(DistinctRowCount.class, Handler.class, new Method[]{BuiltInMethod.DISTINCT_ROW_COUNT.method});

    Double getDistinctRowCount(ImmutableBitSet var1, RexNode var2);

    public interface Handler extends MetadataHandler<DistinctRowCount> {
      Double getDistinctRowCount(RelNode var1, RelMetadataQuery var2, ImmutableBitSet var3, RexNode var4);
    }
  }

  public interface MinRowCount extends Metadata {
    MetadataDef<MinRowCount> DEF = MetadataDef.of(MinRowCount.class, Handler.class, new Method[]{BuiltInMethod.MIN_ROW_COUNT.method});

    Double getMinRowCount();

    public interface Handler extends MetadataHandler<MinRowCount> {
      Double getMinRowCount(RelNode var1, RelMetadataQuery var2);
    }
  }

  public interface MaxRowCount extends Metadata {
    MetadataDef<MaxRowCount> DEF = MetadataDef.of(MaxRowCount.class, Handler.class, new Method[]{BuiltInMethod.MAX_ROW_COUNT.method});

    Double getMaxRowCount();

    public interface Handler extends MetadataHandler<MaxRowCount> {
      Double getMaxRowCount(RelNode var1, RelMetadataQuery var2);
    }
  }

  public interface RowCount extends Metadata {
    MetadataDef<RowCount> DEF = MetadataDef.of(RowCount.class, Handler.class, new Method[]{BuiltInMethod.ROW_COUNT.method});

    Double getRowCount();

    public interface Handler extends MetadataHandler<RowCount> {
      Double getRowCount(RelNode var1, RelMetadataQuery var2);
    }
  }

  public interface NodeTypes extends Metadata {
    MetadataDef<NodeTypes> DEF = MetadataDef.of(NodeTypes.class, Handler.class, new Method[]{BuiltInMethod.NODE_TYPES.method});

    Multimap<Class<? extends RelNode>, RelNode> getNodeTypes();

    public interface Handler extends MetadataHandler<NodeTypes> {
      Multimap<Class<? extends RelNode>, RelNode> getNodeTypes(RelNode var1, RelMetadataQuery var2);
    }
  }

  public interface Distribution extends Metadata {
    MetadataDef<Distribution> DEF = MetadataDef.of(Distribution.class, Handler.class, new Method[]{BuiltInMethod.DISTRIBUTION.method});

    RelDistribution distribution();

    public interface Handler extends MetadataHandler<Distribution> {
      RelDistribution distribution(RelNode var1, RelMetadataQuery var2);
    }
  }

  public interface Collation extends Metadata {
    MetadataDef<Collation> DEF = MetadataDef.of(Collation.class, Handler.class, new Method[]{BuiltInMethod.COLLATIONS.method});

    ImmutableList<RelCollation> collations();

    public interface Handler extends MetadataHandler<Collation> {
      ImmutableList<RelCollation> collations(RelNode var1, RelMetadataQuery var2);
    }
  }

  public interface ColumnUniqueness extends Metadata {
    MetadataDef<ColumnUniqueness> DEF = MetadataDef.of(ColumnUniqueness.class, Handler.class, new Method[]{BuiltInMethod.COLUMN_UNIQUENESS.method});

    Boolean areColumnsUnique(ImmutableBitSet var1, boolean var2);

    public interface Handler extends MetadataHandler<ColumnUniqueness> {
      Boolean areColumnsUnique(RelNode var1, RelMetadataQuery var2, ImmutableBitSet var3, boolean var4);
    }
  }

  public interface UniqueKeys extends Metadata {
    MetadataDef<UniqueKeys> DEF = MetadataDef.of(UniqueKeys.class, Handler.class, new Method[]{BuiltInMethod.UNIQUE_KEYS.method});

    Set<ImmutableBitSet> getUniqueKeys(boolean var1);

    public interface Handler extends MetadataHandler<UniqueKeys> {
      Set<ImmutableBitSet> getUniqueKeys(RelNode var1, RelMetadataQuery var2, boolean var3);
    }
  }

  public interface Selectivity extends Metadata {
    MetadataDef<Selectivity> DEF = MetadataDef.of(Selectivity.class, Handler.class, new Method[]{BuiltInMethod.SELECTIVITY.method});

    Double getSelectivity(RexNode var1);

    public interface Handler extends MetadataHandler<Selectivity> {
      Double getSelectivity(RelNode var1, RelMetadataQuery var2, RexNode var3);
    }
  }
}
