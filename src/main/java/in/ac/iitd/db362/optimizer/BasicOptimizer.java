package in.ac.iitd.db362.optimizer;

import in.ac.iitd.db362.catalog.Catalog;
import in.ac.iitd.db362.api.PlanPrinter;
import in.ac.iitd.db362.operators.ComparisonPredicate;
import in.ac.iitd.db362.operators.EqualityJoinPredicate;
import in.ac.iitd.db362.operators.FilterOperator;
import in.ac.iitd.db362.operators.Operator;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import in.ac.iitd.db362.operators.Predicate;
import in.ac.iitd.db362.operators.ProjectOperator;
import in.ac.iitd.db362.operators.SinkOperator;
import in.ac.iitd.db362.operators.JoinOperator;
import in.ac.iitd.db362.operators.ScanOperator;
// include list
import java.util.List;

/**
 * A basic optimizer implementation. Feel free and be creative in designing your optimizer.
 * Do not change the constructor. Use the catalog for various statistics that are available.
 * For everything in your optimization logic, you are free to do what ever you want.
 * Make sure to write efficient code!
 */
public class BasicOptimizer implements Optimizer {

    // Do not remove or rename logger
    protected final Logger logger = LogManager.getLogger(this.getClass());

    // Do not remove or rename catalog. You'll need it in your optimizer
    private final Catalog catalog;

    /**
     * DO NOT CHANGE THE CONSTRUCTOR!
     *
     * @param catalog
     */
    public BasicOptimizer(Catalog catalog) {
        this.catalog = catalog;
    }

    private Set<String> collectColumns(Operator op) {
        if (op instanceof ScanOperator) {
            if (((ScanOperator)op).getSchema() == null) {
                return new HashSet<>();
            }
            return new HashSet<>(((ScanOperator)op).getSchema());
        }
        if (op instanceof ProjectOperator) {
            return new HashSet<>(((ProjectOperator)op).getProjectedColumns());
            // return collectColumns(((ProjectOperator)op).getChild());
        }
        if (op instanceof FilterOperator) {
            return collectColumns(((FilterOperator)op).getChild());
        }
        if (op instanceof JoinOperator) {
            JoinOperator j = (JoinOperator)op;
            Set<String> cols = collectColumns(j.getLeftChild());
            cols.addAll(collectColumns(j.getRightChild()));
            return cols;
        }
        if (op instanceof SinkOperator) {
            return collectColumns(((SinkOperator)op).getChild());
        }
        return Collections.emptySet();
    }

    private Operator pushFilterBelow(Operator child, Predicate pred){
        if (child instanceof ProjectOperator) {
            ProjectOperator p = (ProjectOperator) child;
            return new ProjectOperator(
                pushFilterBelow(p.getChild(), pred),
                p.getProjectedColumns(),
                p.isDistinct()
            );
        }

        if (child instanceof FilterOperator) {
            FilterOperator f = (FilterOperator) child;
            return new FilterOperator(pushFilterBelow(f.getChild(),pred), f.getPredicate());
        }

        if (child instanceof JoinOperator) {
            JoinOperator j = (JoinOperator) child;
            ComparisonPredicate cp = (ComparisonPredicate) pred;
            // since the pridcate will obviously be a comparison predicate
            String col = cp.getLeftOperand() instanceof String
                     ? (String)cp.getLeftOperand()
                     : (String)cp.getRightOperand();
            Set<String> leftCols = collectColumns(j.getLeftChild());
            Set<String> rightCols = collectColumns(j.getRightChild());
            if (leftCols.isEmpty() || rightCols.isEmpty()) {
                return new FilterOperator(child, pred);
            }
            boolean inLeft = leftCols.contains(col);
            boolean inRight = rightCols.contains(col);
            if (inLeft && !inRight) {
                return new JoinOperator(
                    pushFilterBelow(j.getLeftChild(), pred),
                    j.getRightChild(),
                    j.getPredicate()
                );
            }
            else if (inRight && !inLeft) {
                return new JoinOperator(
                    j.getLeftChild(),
                    pushFilterBelow(j.getRightChild(), pred),
                    j.getPredicate()
                );
            }
        }
        if (child instanceof ScanOperator) {
            return new FilterOperator(child, pred);
        }
        return new FilterOperator(child, pred);
    }

    // Push Down filters to the leaves of the plan
    private Operator pushDownFilters(Operator op){
        if (op instanceof FilterOperator) {
            FilterOperator f = (FilterOperator) op;
            Operator pushedChild = pushDownFilters(f.getChild());
            return pushFilterBelow(pushedChild, f.getPredicate());
        }
        if (op instanceof ProjectOperator) {
            ProjectOperator p = (ProjectOperator) op;
            Operator c = pushDownFilters(p.getChild());
            return new ProjectOperator(c, p.getProjectedColumns(), p.isDistinct());
        }
        if (op instanceof JoinOperator) {
            JoinOperator j = (JoinOperator) op;
            Operator leftChild = pushDownFilters(j.getLeftChild());
            Operator rightChild = pushDownFilters(j.getRightChild());
            return new JoinOperator(leftChild, rightChild, j.getPredicate());
        }
        if (op instanceof SinkOperator) {
            SinkOperator s = (SinkOperator) op;
            Operator c = pushDownFilters(s.getChild());
            return new SinkOperator(c, s.getOutputFile());
        }
        return op;
    }
    // Pushing down the projects
    private Operator pushProjectsBelow(Operator child, List<String> pCols, boolean distinct) {
        if (child instanceof ProjectOperator) {
            boolean notCase = !distinct && ((ProjectOperator) child).isDistinct();
            if (!notCase) {
                // remove the below project
                Operator pChild = ((ProjectOperator) child).getChild();
                return new ProjectOperator(pChild, pCols, distinct);
            }
        }

        if (child instanceof JoinOperator) {
            JoinOperator j = (JoinOperator) child;
            EqualityJoinPredicate p = (EqualityJoinPredicate) j.getPredicate();
            String lCol = p.getLeftColumn();
            String rCol = p.getRightColumn();
            boolean leftIn_Pcols = pCols.contains(lCol);
            boolean rightIn_Pcols = pCols.contains(rCol);
            if (leftIn_Pcols && rightIn_Pcols) {
                return new JoinOperator(
                    pushProjectsBelow(j.getLeftChild(), pCols, distinct),
                    pushProjectsBelow(j.getRightChild(), pCols, distinct),
                    j.getPredicate()
                );
            }
        }
        if (child instanceof FilterOperator) {
            return new ProjectOperator(child, pCols, distinct);
        }
        if (child instanceof ScanOperator) {
            return new ProjectOperator(child, pCols, distinct);
        }
        return new ProjectOperator(child, pCols, distinct);
    }

    private Operator pushDownProjects(Operator op) {
        if (op instanceof ProjectOperator) {
            ProjectOperator p = (ProjectOperator) op;
            Operator pushedChild = pushDownProjects(p.getChild());
            return pushProjectsBelow(pushedChild, p.getProjectedColumns(), p.isDistinct());
        }
        if (op instanceof SinkOperator) {
            SinkOperator s = (SinkOperator) op;
            Operator c = pushDownProjects(s.getChild());
            return new SinkOperator(c, s.getOutputFile());
        }
        if (op instanceof JoinOperator) {
            JoinOperator j = (JoinOperator) op;
            Operator leftChild = pushDownProjects(j.getLeftChild());
            Operator rightChild = pushDownProjects(j.getRightChild());
            return new JoinOperator(leftChild, rightChild, j.getPredicate());
        }
        return op;
    }
    /**
     * Basic optimization that currently does not modify the plan. Your goal is to come up with
     * an optimization strategy that should find an optimal plan. Come up with your own ideas or adopt the ones
     * discussed in the lecture to efficiently enumerate plans, a search strategy along with a cost model.
     *
     * @param plan The original query plan.
     * @return The (possibly) optimized query plan.
     */
    @Override
    public Operator optimize(Operator plan) {
        logger.info("Optimizing Plan:\n{}", PlanPrinter.getPlanString(plan));
        // TODO: Implement me!
        // For now, we simply return the plan unmodified.
        // First we will push down the filters
        Operator optimizedPlan = pushDownFilters(plan);
        // Now we will push down the project operators
        optimizedPlan = pushDownProjects(optimizedPlan);
        return optimizedPlan;
    }
}
