package ai.dataeng.sqml.planner;

import com.google.common.base.Preconditions;

import java.util.*;

/**
 * Iterates over a {@link LogicalPlanImpl} from source to sink. It ensures that all inputs to a node are visisted
 * before the node itself is visited.
 */
public class LogicalPlanIterator implements Iterator<LogicalPlanImpl.Node> {

    private final Set<LogicalPlanImpl.Node> visited = new HashSet<>();
    private final Set<LogicalPlanImpl.Node> frontier = new HashSet<>();

    public LogicalPlanIterator(LogicalPlanImpl logicalPlan) {
        this(logicalPlan.sourceNodes);
    }

    public LogicalPlanIterator(Collection<? extends LogicalPlanImpl.Node> sourceNodes) {
        frontier.addAll(sourceNodes);
    }

    @Override
    public boolean hasNext() {
        return !frontier.isEmpty();
    }

    @Override
    public LogicalPlanImpl.Node next() {
        Preconditions.checkArgument(hasNext());
        LogicalPlanImpl.Node selected = null;
        for (LogicalPlanImpl.Node node : frontier) {
            boolean allInputsVisited = true;
            for (LogicalPlanImpl.Node input : (List<LogicalPlanImpl.Node>)node.getInputs()) {
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
        for (LogicalPlanImpl.Node consumer : (List<LogicalPlanImpl.Node>)selected.getConsumers()) {
            frontier.add(consumer);
        }
        return selected;
    }
}
