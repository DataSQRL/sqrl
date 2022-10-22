package ai.datasqrl.plan.calcite.rules;

public abstract class ExecutionStageException extends RuntimeException {

    protected ExecutionStageException() {
        super();
    }

    protected ExecutionStageException(String msg) {
        super(msg);
    }

    public static class StageChange extends ExecutionStageException {

    }

    public static class StageFinding extends ExecutionStageException {

    }

    public static class StageIncompatibility extends ExecutionStageException {

        public StageIncompatibility(String msg) {
            super(msg);
        }

    }

}
