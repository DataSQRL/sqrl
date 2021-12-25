package ai.dataeng.sqml.execution.flink.process.aggregates;


import ai.dataeng.sqml.execution.flink.process.Row;

public interface AggsHandleFunction extends Function {

    void open(/*StateDataViewStore store*/) throws Exception;

    Row getValue() throws Exception;
    
    void accumulate(Row input) throws Exception;

    /**
     * Retracts the input values from the accumulators.
     *
     * @param input input values bundled in a row
     */
    void retract(Row input) throws Exception;

    /**
     * Merges the other accumulators into current accumulators.
     *
     * @param accumulators The other row of accumulators
     */
    void merge(Row accumulators) throws Exception;

    /**
     * Set the current accumulators (saved in a row) which contains the current aggregated results.
     * In streaming: accumulators are store in the state, we need to restore aggregate buffers from
     * state. In batch: accumulators are store in the hashMap, we need to restore aggregate buffers
     * from hashMap.
     *
     * @param accumulators current accumulators
     */
    void setAccumulators(Row accumulators) throws Exception;

    /** Resets all the accumulators. */
    void resetAccumulators() throws Exception;

    /**
     * Gets the current accumulators (saved in a row) which contains the current aggregated results.
     *
     * @return the current accumulators
     */
    Row getAccumulators() throws Exception;

    /**
     * Initializes the accumulators and save them to a accumulators row.
     *
     * @return a row of accumulators which contains the aggregated results
     */
    Row createAccumulators() throws Exception;

    /** Cleanup for the retired accumulators state. */
    void cleanup() throws Exception;

    /**
     * Tear-down method for this function. It can be used for clean up work. By default, this method
     * does nothing.
     */
    void close() throws Exception;
}
