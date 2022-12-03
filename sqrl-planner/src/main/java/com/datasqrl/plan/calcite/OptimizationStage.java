package com.datasqrl.plan.calcite;

import com.datasqrl.plan.calcite.rules.DAGExpansionRule;
import com.datasqrl.plan.calcite.rules.SQRLPrograms;
import com.datasqrl.plan.calcite.rules.SqrlRelMetadataProvider;
import lombok.Value;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * An {@link OptimizationStage}
 */
@Value
public class OptimizationStage {

    private static final List<OptimizationStage> ALL_STAGES = new ArrayList<>();

    private final int index;
    private final String name;
    private final Program program;
    private final Optional<RelTrait> trait;

    public OptimizationStage(String name, Program program, Optional<RelTrait> trait) {
        this.name = name;
        this.program = program;
        this.trait = trait;
        synchronized (ALL_STAGES) {
            this.index = ALL_STAGES.size();
            ALL_STAGES.add(index,this);
        }
    }

    public static List<Program> getAllPrograms() {
        return ALL_STAGES.stream().map(OptimizationStage::getProgram).collect(Collectors.toList());
    }

    /*
    ====== DEFINITION OF ACTUAL STAGES
     */

    public static final OptimizationStage PUSH_FILTER_INTO_JOIN = new OptimizationStage("PushFilterIntoJoin",
            Programs.hep(List.of(
                    CoreRules.FILTER_INTO_JOIN
                    ), false, DefaultRelMetadataProvider.INSTANCE),
            Optional.empty());

    public static final OptimizationStage READ_DAG_STITCHING = new OptimizationStage("ReadDAGExpansion",
            Programs.hep(List.of(new DAGExpansionRule.ReadOnly()),
                    false, SqrlRelMetadataProvider.INSTANCE), Optional.empty());

    public static final OptimizationStage WRITE_DAG_STITCHING = new OptimizationStage("WriteDAGExpansion",
            Programs.hep(List.of(new DAGExpansionRule.WriteOnly()), false, SqrlRelMetadataProvider.INSTANCE),
            Optional.empty());

    public static final OptimizationStage READ2WRITE_STITCHING = new OptimizationStage("Read2WriteAdjustment",
            Programs.hep(List.of(new DAGExpansionRule.Read2Write()), false, SqrlRelMetadataProvider.INSTANCE),
            Optional.empty());

    public static final OptimizationStage VOLCANO = new OptimizationStage("Volcano",
        SQRLPrograms.ENUMERABLE_VOLCANO, Optional.of(EnumerableConvention.INSTANCE)
        );

    public static final OptimizationStage READ_QUERY_OPTIMIZATION = new OptimizationStage("ReadQueryOptimization",
            SQRLPrograms.ENUMERABLE_VOLCANO, Optional.of(EnumerableConvention.INSTANCE)
    );

    public static final OptimizationStage PUSH_DOWN_FILTERS = new OptimizationStage("PushDownFilters",
            Programs.hep(List.of(
                    CoreRules.FILTER_INTO_JOIN,
                    CoreRules.FILTER_MERGE,
                    CoreRules.FILTER_AGGREGATE_TRANSPOSE,
                    CoreRules.FILTER_PROJECT_TRANSPOSE,
                    CoreRules.FILTER_TABLE_FUNCTION_TRANSPOSE,
                    CoreRules.FILTER_CORRELATE,
                    CoreRules.FILTER_SET_OP_TRANSPOSE
            ), false, SqrlRelMetadataProvider.INSTANCE),
            Optional.empty());

    //Enumerable
//    public static final OptimizationStage SQRL_ENUMERABLE_HEP = new OptimizationStage("SQRL2Enumerable",
//            Programs.hep(List.of(new SqrlExpansionRelRule()), false, DefaultRelMetadataProvider.INSTANCE),
//            Optional.empty());
//    public static final OptimizationStage STANDARD_ENUMERABLE_RULES = new OptimizationStage("standardEnumerable",
//            Programs.sequence(
//                    Programs.subQuery(DefaultRelMetadataProvider.INSTANCE),
//                    SQRLPrograms.ENUMERABLE_VOLCANO,
//                    Programs.calc(DefaultRelMetadataProvider.INSTANCE),
//                    Programs.hep(
//                            List.of(new SqrlDataSourceToEnumerableConverterRule()), false, DefaultRelMetadataProvider.INSTANCE
//                    )
//            ),
//            Optional.of(EnumerableConvention.INSTANCE));
//    public static final List<OptimizationStage> ENUMERABLE_STAGES = ImmutableList.of(SQRL_ENUMERABLE_HEP,STANDARD_ENUMERABLE_RULES);

}
