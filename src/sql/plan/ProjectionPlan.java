package sql.plan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ProjectionPlan implements LogicalPlan {
    private final LogicalPlan input;
    private final List<Integer> columnIndexes;
    private final List<String> columnNames;

    public ProjectionPlan(LogicalPlan input, List<Integer> columnIndexes, List<String> columnNames) {
        this.input = input;
        this.columnIndexes = Collections.unmodifiableList(new ArrayList<>(columnIndexes));
        this.columnNames = Collections.unmodifiableList(new ArrayList<>(columnNames));
    }

    public LogicalPlan getInput() {
        return input;
    }

    public List<Integer> getColumnIndexes() {
        return columnIndexes;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    @Override
    public String toString() {
        return "ProjectionPlan{" +
                "columnIndexes=" + columnIndexes +
                ", columnNames=" + columnNames +
                ", input=" + input +
                '}';
    }
}
