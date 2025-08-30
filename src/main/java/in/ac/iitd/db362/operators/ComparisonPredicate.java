package in.ac.iitd.db362.operators;

import in.ac.iitd.db362.storage.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Note: ONLY IMPLEMENT THE EVALUATE METHOD.
 * TODO: Implement the evaluate() method
 *
 * DO NOT CHANGE the constructor or existing member variables.
 *
 * A comparison predicate for simple atomic predicates.
 */
public class ComparisonPredicate implements Predicate {

    protected final static Logger logger = LogManager.getLogger();
    private final Object leftOperand;   // Either a constant or a column reference (String)
    private final String operator;        // One of: =, >, >=, <, <=, !=
    private final Object rightOperand;  // Either a constant or a column reference (String)

    public ComparisonPredicate(Object leftOperand, String operator, Object rightOperand) {
        this.leftOperand = leftOperand;
        this.operator = operator;
        this.rightOperand = rightOperand;
    }

    private Object resolveOperand(Object operand, Tuple tuple) {
        if (operand instanceof String && tuple.getSchema().contains(operand)) {
            return tuple.get((String) operand);
        } else {
            return operand;
        }
    }
    /**
     * Evaluate a tuple
     * @param tuple the tuple to evaluate
     * @return return true if leftOperator operator righOperand holds in that tuple
     */
    @Override
    public boolean evaluate(Tuple tuple) {
        // DO NOT REMOVE LOGGING ---
        logger.trace("Evaluating tuple " + tuple.getValues() + " with schema " + tuple.getSchema());
        logger.trace("[Predicate] " + leftOperand + " " + operator + " " + rightOperand);
        // -------------------------
        // System.out.println("printing ----");
        //TODO: Implement me!
        Object leftValue = resolveOperand(leftOperand, tuple);
        // Object rightValue = resolveOperand(rightOperand, tuple);
        Object rightValue = rightOperand;
    
        if (leftValue == null || rightValue == null) return false;
        // System.out.println("Coming till this point");
        if (leftValue instanceof Number && rightValue instanceof Number) {
            double l = ((Number) leftValue).doubleValue();
            double r = ((Number) rightValue).doubleValue();
            switch (operator) {
                case "=":
                    return l == r;
                case "!=":
                    return l != r;
                case ">":
                    return l > r;
                case ">=":
                    return l >= r;
                case "<":
                    return l < r;
                case "<=":
                    return l <= r;
                default:
                    throw new RuntimeException("Unknown operator: " + operator);
            }
        } else {
            String l = leftValue.toString();
            String r = rightValue.toString();
            int cmp = l.compareTo(r);
            switch (operator) {
                case "=":
                    return cmp == 0;
                case "!=":
                    return cmp != 0;
                case ">":
                    return cmp > 0;
                case ">=":
                    return cmp >= 0;
                case "<":
                    return cmp < 0;
                case "<=":
                    return cmp <= 0;
                default:
                    throw new RuntimeException("Unknown operator: " + operator);
            }
        }
    }

    // DO NOT REMOVE these functions! ---
    @Override
    public String toString() {
        return "ComparisonPredicate[" +
                "leftOperand=" + leftOperand +
                ", operator='" + operator + '\'' +
                ", rightOperand=" + rightOperand +
                ']';
    }
    public Object getLeftOperand() {
        return leftOperand;
    }

    public String getOperator() {
        return operator;
    }
    public Object getRightOperand() {
        return rightOperand;
    }

}
