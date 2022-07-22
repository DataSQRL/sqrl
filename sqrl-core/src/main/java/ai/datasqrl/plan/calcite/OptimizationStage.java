package ai.datasqrl.plan.calcite;

import ai.datasqrl.plan.calcite.memory.rule.SqrlDataSourceToEnumerableConverterRule;
import ai.datasqrl.plan.calcite.sqrl.rules.SQRLPrograms;
import ai.datasqrl.plan.calcite.sqrl.rules.SqrlExpansionRelRule;
import com.google.common.collect.ImmutableList;
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

    public static final OptimizationStage PUSH_FILTER_INTO_JOIN = new OptimizationStage("PushDownProjections",
            Programs.hep(List.of(
                    CoreRules.FILTER_INTO_JOIN
                    ), false, DefaultRelMetadataProvider.INSTANCE),
            Optional.empty());

    //Enumerable
    public static final OptimizationStage SQRL_ENUMERABLE_HEP = new OptimizationStage("SQRL2Enumerable",
            Programs.hep(List.of(new SqrlExpansionRelRule()), false, DefaultRelMetadataProvider.INSTANCE),
            Optional.empty());
    public static final OptimizationStage STANDARD_ENUMERABLE_RULES = new OptimizationStage("standardEnumerable",
            Programs.sequence(
                    Programs.subQuery(DefaultRelMetadataProvider.INSTANCE),
                    SQRLPrograms.ENUMERABLE_VOLCANO,
                    Programs.calc(DefaultRelMetadataProvider.INSTANCE),
                    Programs.hep(
                            List.of(new SqrlDataSourceToEnumerableConverterRule()), false, DefaultRelMetadataProvider.INSTANCE
                    )
            ),
            Optional.of(EnumerableConvention.INSTANCE));
    public static final List<OptimizationStage> ENUMERABLE_STAGES = ImmutableList.of(SQRL_ENUMERABLE_HEP,STANDARD_ENUMERABLE_RULES);

}
