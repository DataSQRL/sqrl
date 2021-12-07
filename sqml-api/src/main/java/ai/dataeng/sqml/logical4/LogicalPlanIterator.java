package ai.dataeng.sqml.logical4;

import com.google.common.base.Preconditions;

import java.util.*;

public class LogicalPlanIterator implements Iterator<LogicalPlan.Node> {

    private final Set<LogicalPlan.Node> visited = new HashSet<>();
    private final Set<LogicalPlan.Node> frontier = new HashSet<>();

    public LogicalPlanIterator(LogicalPlan logicalPlan) {
        this(logicalPlan.sourceNodes);
    }

    public LogicalPlanIterator(Collection<? extends LogicalPlan.Node> sourceNodes) {
        frontier.addAll(sourceNodes);
    }

    @Override
    public boolean hasNext() {
        return !frontier.isEmpty();
    }

    @Override
    public LogicalPlan.Node next() {
        Preconditions.checkArgument(hasNext());
        LogicalPlan.Node selected = null;
        for (LogicalPlan.Node node : frontier) {
            boolean allInputsVisited = true;
            for (LogicalPlan.Node input : (List<LogicalPlan.Node>)node.getInputs()) {
                if (!visited.contains(input)) {
                    allInputsVisited=false;
                    break;
                }
            }
            if (allInputsVisited) {
                selected = node;
                break;
            }
        }
        Preconditions.checkArgument(selected!=null,"Logical Plan is broken");
        visited.add(selected);
        frontier.remove(selected);
        for (LogicalPlan.Node consumer : (List<LogicalPlan.Node>)selected.getConsumers()) {
            frontier.add(consumer);
        }
        return selected;
    }
}
