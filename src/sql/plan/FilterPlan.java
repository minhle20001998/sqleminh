package sql.plan;

import sql.binder.BoundComparisonExpression;

public class FilterPlan implements LogicalPlan {
    private final LogicalPlan input;
    private final BoundComparisonExpression predicate;

    public FilterPlan(LogicalPlan input, BoundComparisonExpression predicate) {
        this.input = input;
        this.predicate = predicate;
    }

    public LogicalPlan getInput() {
        return input;
    }

    public BoundComparisonExpression getPredicate() {
        return predicate;
    }

    @Override
    public String toString() {
        return "FilterPlan{" +
                "predicate=" + predicate +
                ", input=" + input +
                '}';
    }
}
