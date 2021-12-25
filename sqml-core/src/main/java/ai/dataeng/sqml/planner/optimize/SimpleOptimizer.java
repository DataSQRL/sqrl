package ai.dataeng.sqml.planner.optimize;

import ai.dataeng.sqml.planner.LogicalPlanImpl;
import ai.dataeng.sqml.planner.LogicalPlanIterator;
import ai.dataeng.sqml.planner.LogicalPlanUtil;
import ai.dataeng.sqml.planner.operator.AccessNode;

import java.util.ArrayList;
import java.util.List;

public class SimpleOptimizer implements LogicalPlanOptimizer {

    @Override
    public Result optimize(LogicalPlanImpl logicalPlan) {
        List<LogicalPlanImpl.Node> streamSources = new ArrayList<>();
        List<MaterializeSource> readSources = new ArrayList<>();

        //Draw the materialization boundary at each QueryNode, i.e. materialize everything
        LogicalPlanIterator lpiter = new LogicalPlanIterator(logicalPlan);
        while (lpiter.hasNext()) {
            LogicalPlanImpl.Node node = lpiter.next();
            if (LogicalPlanUtil.isSource(node)) streamSources.add(node);
            if (node instanceof AccessNode) {
                AccessNode qnode = (AccessNode) node;
                LogicalPlanImpl.RowNode input = qnode.getInput();
                MaterializeSink sinkNode = new MaterializeSink(input, qnode.getTable());
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
