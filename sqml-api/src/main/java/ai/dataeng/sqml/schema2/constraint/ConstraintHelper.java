package ai.dataeng.sqml.schema2.constraint;

import java.util.Optional;

public class ConstraintHelper {

    public static boolean isNonNull(Iterable<Constraint> contraints) {
        return getConstraint(contraints,NotNull.class).isPresent();
    }

    public static<C extends Constraint> Optional<C> getConstraint(Iterable<Constraint> contraints, Class<C> constraintClass) {
        for (Constraint c : contraints) {
            if (constraintClass.isInstance(c)) return Optional.of((C)c);
        }
        return Optional.empty();
    }

}
