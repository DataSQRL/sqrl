package ai.datasqrl.plan.calcite.sqrl.hints;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;
import java.util.Optional;

public interface SqrlHint {

    RelHint getHint();

    default RelNode addHint(Join join) {
        return join.attachHints(List.of(getHint()));
    }

    static<H extends SqrlHint> Optional<H> fromRel(RelNode node, SqrlHint.Constructor<H> hintConstructor) {
        if (node instanceof Join) {
            return ((Hintable)node).getHints().stream()
                    .filter(h -> h.hintName.equalsIgnoreCase(hintConstructor.getName()))
                    .filter(h -> h.inheritPath.isEmpty()) //we only want the hint on that particular join, not inherited ones
                    .findFirst().map(hintConstructor::fromHint);
        }
        return Optional.empty();
    }


    public interface Constructor<H extends SqrlHint> {

        public String getName();

        public H fromHint(RelHint hint);

    }

}
