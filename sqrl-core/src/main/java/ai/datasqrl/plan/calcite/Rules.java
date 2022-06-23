package ai.datasqrl.plan.calcite;

import ai.datasqrl.plan.calcite.memory.rule.SqrlDataSourceToEnumerableConverterRule;
import ai.datasqrl.plan.calcite.sqrl.rules.SqrlExpansionRelRule;
import java.util.List;
import java.util.Optional;
import lombok.Value;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;

/**
 * This class contains a list of stages in the optimizer and each stage’s rules. We also take
 * this opportunity to create a Program to convert our logical plan to a physical one. To do this,
 * we’re using a combination of Calcite’s default optimizer and our own custom rules.
 */
public class Rules {
  public static final Stage SQRL_ENUMERABLE_HEP = new Stage(0, Optional.empty());
  public static final Stage STANDARD_Enumerable_RULES = new Stage(1, Optional.of(EnumerableConvention.INSTANCE));

  public static final List<Stage> ENUMERABLE_STAGES =
      List.of(SQRL_ENUMERABLE_HEP, STANDARD_Enumerable_RULES);

  public static Program getSqrlEnumerableRules() {
    return Programs.hep(List.of(new SqrlExpansionRelRule()), false, DefaultRelMetadataProvider.INSTANCE);
  }

  /**
   * The Stage class is just a simple data container, but it’s important to note that we’re
   * providing an optional RelTrait, which is used to define which physical traits we’re
   * interested in. If we don’t provide the optional RelTrait, then the optimizer will try to
   * optimize to any physical traits that the logical plan is able to support.
   */
  @Value
  public static class Stage {
    int stage;
    Optional<RelTrait> trait;

    public RelTraitSet applyStageTrait(RelTraitSet traitSet) {
      return trait.map(traitSet::replace).orElse(traitSet);
    }
  }

  static Program volcano =
      (planner, rel, requiredOutputTraits, materializations, lattices) -> {
        if (rel.getTraitSet().equals(requiredOutputTraits)) {
          return rel;
        }

        RelNode rel2 = planner.changeTraits(rel, requiredOutputTraits);
        planner.setRoot(rel2);

        final RelOptPlanner planner2 = planner.chooseDelegate();
        final RelNode rootRel3 = planner2.findBestExp();
        assert rootRel3 != null : "could not implement exp";
        return rootRel3;
      };

  static Program standardProgram = Programs.sequence(
      Programs.subQuery(DefaultRelMetadataProvider.INSTANCE),
      volcano,
      Programs.calc(DefaultRelMetadataProvider.INSTANCE),
      Programs.hep(
        List.of(new SqrlDataSourceToEnumerableConverterRule()), false, DefaultRelMetadataProvider.INSTANCE
      )
  );

  public static List<Program> programs() {
    return List.of(
        getSqrlEnumerableRules(),
        standardProgram
      );
  }
}
