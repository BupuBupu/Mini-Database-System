package in.ac.iitd.db362.operators;

import in.ac.iitd.db362.storage.Tuple;

import java.util.*;

/**
 * The join operator performs a Hash Join.
 * TODO: Implement the open(), next(), and close() methods.
 *
 * Do not change the constructor and member variables or getters
 * Do not remove logging! otherwise your test cases will fail!
 */
public class JoinOperator extends OperatorBase implements Operator {
    private Operator leftChild;
    private Operator rightChild;
    private JoinPredicate predicate;

    private List<Tuple> leftBuffer, rightBuffer;
    private Map<Object, List<Tuple>> hashTable;
    private boolean hashLeftSide;
    private int probeIdx, buildKeyIdx, probeKeyIdx;
    private int matchIdx;
    private List<Tuple> currentMatches;

    public JoinOperator(Operator leftChild, Operator rightChild, JoinPredicate predicate) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.predicate = predicate;
    }

    @Override
    public void open() {
        // Do not remove logging--
        logger.trace("Open()");
        // ----------------------
        leftChild.open();
        leftBuffer = new ArrayList<>();
        Tuple t;
        while ((t = leftChild.next()) != null) {
            leftBuffer.add(t);
        }

        rightChild.open();
        rightBuffer = new ArrayList<>();
        while ((t = rightChild.next()) != null) {
            rightBuffer.add(t);
        }

        hashLeftSide = leftBuffer.size() <= rightBuffer.size();
        List<Tuple> buildSide = hashLeftSide ? leftBuffer : rightBuffer;
        List<Tuple> probeSide = hashLeftSide ? rightBuffer : leftBuffer;

        EqualityJoinPredicate eq = (EqualityJoinPredicate) predicate;
        String leftCol  = eq.getLeftColumn();
        String rightCol = eq.getRightColumn();
        if (hashLeftSide) {
            buildKeyIdx = buildSide.get(0).getSchema().indexOf(leftCol);
            probeKeyIdx = probeSide.get(0).getSchema().indexOf(rightCol);
        } else {
            buildKeyIdx = buildSide.get(0).getSchema().indexOf(rightCol);
            probeKeyIdx = probeSide.get(0).getSchema().indexOf(leftCol);
        }

        hashTable = new HashMap<>();
        for (Tuple buildT : buildSide) {
            Object key = buildT.get(buildKeyIdx);
            hashTable.computeIfAbsent(key, k -> new ArrayList<>()).add(buildT);
        }

        probeIdx        = 0;
        matchIdx        = 0;
        currentMatches  = Collections.emptyList();

    }

    @Override
    public Tuple next() {
        // Do not remove logging--
        logger.trace("Next()");
        List<Tuple> probeSide = hashLeftSide ? rightBuffer : leftBuffer;

        while (true) {
            if (matchIdx >= currentMatches.size()) {
                if (probeIdx >= probeSide.size()) return null;
                Tuple probeT = probeSide.get(probeIdx);
                currentMatches = hashTable.getOrDefault(
                    probeT.get(probeKeyIdx),
                    Collections.emptyList()
                );
                matchIdx = 0;
                probeIdx++;
                continue;
            }

            Tuple buildT = currentMatches.get(matchIdx++);
            Tuple probeT = probeSide.get(probeIdx - 1);
            Tuple leftT  = hashLeftSide ? buildT : probeT;
            Tuple rightT = hashLeftSide ? probeT  : buildT;

            if (predicate.evaluate(leftT, rightT)) {
                List<Object> vals = new ArrayList<>(leftT.getValues());
                vals.addAll(rightT.getValues());
                List<String> sch  = new ArrayList<>(leftT.getSchema());
                sch.addAll(rightT.getSchema());
                return new Tuple(vals, sch);
            }
        }
    }

    @Override
    public void close() {
        // Do not remove logging ---
        logger.trace("Close()");
        // ------------------------
        leftChild.close();
        rightChild.close();
        leftBuffer     = null;
        rightBuffer    = null;
        hashTable      = null;
        currentMatches = null;
        //TODO: Implement me!

    }


    // Do not remove these methods!
    public Operator getLeftChild() {
        return leftChild;
    }

    public Operator getRightChild() {
        return rightChild;
    }

    public JoinPredicate getPredicate() {
        return predicate;
    }
}
