package ai.dataeng.sqml.physical.flink;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class AggregationProcess extends KeyedProcessFunction<Row,RowUpdate,RowUpdate> {



    @Override
    public void processElement(RowUpdate rowUpdate, KeyedProcessFunction<Row, RowUpdate, RowUpdate>.Context context,
                               Collector<RowUpdate> collector) throws Exception {
        //TODO: to be implemented
    }
}
