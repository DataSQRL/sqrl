package ai.dataeng.sqml.optimizer;

import ai.dataeng.sqml.logical4.LogicalPlan;
import ai.dataeng.sqml.logical4.LogicalPlanIterator;
import ai.dataeng.sqml.logical4.LogicalPlanUtil;
import ai.dataeng.sqml.logical4.AccessNode;

import java.util.ArrayList;
import java.util.List;

public class SimpleOptimizer implements LogicalPlanOptimizer {

    @Override
    public Result optimize(LogicalPlan logicalPlan) {
        List<LogicalPlan.Node> streamSources = new ArrayList<>();
        List<MaterializeSource> readSources = new ArrayList<>();

        //Draw the materialization boundary at each QueryNode, i.e. materialize everything
        LogicalPlanIterator lpiter = new LogicalPlanIterator(logicalPlan);
        while (lpiter.hasNext()) {
            LogicalPlan.Node node = lpiter.next();
            if (LogicalPlanUtil.isSource(node)) streamSources.add(node);
            if (node instanceof AccessNode) {
                AccessNode qnode = (AccessNode) node;
                LogicalPlan.RowNode input = qnode.getInput();
                MaterializeSink sinkNode = new MaterializeSink(input, qnode.getTable().getId());
                input.replaceConsumer(qnode, sinkNode);
                MaterializeSource sourceNode = sinkNode.getSource();
                qnode.replaceInput(input, sourceNode);
                sourceNode.addConsumer(qnode);
                readSources.add(sourceNode);
            }
        }
        return new Result(streamSources,readSources);
    }

}
