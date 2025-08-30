package in.ac.iitd.db362.operators;

import in.ac.iitd.db362.storage.Tuple;

import java.util.*;

/**
 * Implementation of a simple project operator that implements the operator interface.
 *
 *
 * TODO: Implement the open(), next(), and close() methods!
 * Do not change the constructor or existing member variables.
 */
public class ProjectOperator extends OperatorBase implements Operator {
    private Operator child;
    private List<String> projectedColumns;
    private boolean distinct;

    private Set<List<Object>> seenTuples;
    
    /**
     * Project operator. If distinct is set to true, it does duplicate elimination
     * @param child
     * @param projectedColumns
     * @param distinct
     */
    public ProjectOperator(Operator child, List<String> projectedColumns, boolean distinct) {
        this.child = child;
        this.projectedColumns = projectedColumns;
        this.distinct = distinct;
    }

    @Override
    public void open() {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Open()");
        // -------------------------
        child.open();
        if (distinct) seenTuples = new HashSet<>();
        // TODO: Implement me!
    }

    @Override
    public Tuple next() {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Next()");
        // ------------------------

        //TODO: Implement me!
        Tuple input;
        while ((input = child.next()) != null) {
            List<String> inSchema = input.getSchema();
            List<Object> outValues = new ArrayList<>();
            List<String> outSchema = new ArrayList<>();

            for (String col : projectedColumns) {
                if (inSchema.contains(col)) {
                    outSchema.add(col);
                    outValues.add(input.get(col));
                }
            }

            if (distinct) {
                if (seenTuples.add(outValues)) {
                    return new Tuple(outValues, outSchema);
                }
            } else {
                return new Tuple(outValues, outSchema);
            }
        }
        return null;

        // remove me after implementation
        // throw new RuntimeException("Method not yet implemented");
    }

    @Override
    public void close() {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Close()");
        // ------------------------
        child.close();
        seenTuples = null;
        // TODO: Implement me!
    }

    // do not remvoe these methods!
    public Operator getChild() {
        return child;
    }

    public List<String> getProjectedColumns() {
        return projectedColumns;
    }

    public boolean isDistinct() {
        return distinct;
    }
}
