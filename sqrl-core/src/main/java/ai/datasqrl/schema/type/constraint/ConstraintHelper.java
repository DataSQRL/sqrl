package ai.datasqrl.schema.type.constraint;

import java.util.Optional;

public class ConstraintHelper {

    public static boolean isNonNull(Iterable<Constraint> constraints) {
        return getConstraint(constraints,NotNull.class).isPresent();
    }

    public static Cardinality getCardinality(Iterable<Constraint> constraints) {
        return ConstraintHelper.getConstraint(constraints, Cardinality.class).orElse(Cardinality.UNCONSTRAINED);
    }

    public static<C extends Constraint> Optional<C> getConstraint(Iterable<Constraint> contraints, Class<C> constraintClass) {
        for (Constraint c : contraints) {
            if (constraintClass.isInstance(c)) return Optional.of((C)c);
        }
        return Optional.empty();
    }

}
