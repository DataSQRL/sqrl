package ai.datasqrl.execute.flink;

public enum StreamType {

    /**
     * Records passed between nodes in the logical plan are only additions, i.e. we will never encounter retractions.
     * This is true for event streams and when executing a plan in batch mode.
     */
    APPEND,
    /**
     * Records passed between nodes can contain additions and retractions.
     */
    RETRACT;

}
