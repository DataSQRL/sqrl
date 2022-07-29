package ai.datasqrl.plan.global;

public enum MaterializationStrategy {

    MUST, SHOULD, SHOULD_NOT, CANNOT;


    public boolean isMaterialize() {
        switch (this) {
            case MUST:
            case SHOULD:
                return true;
            default: return false;
        }
    }

}
