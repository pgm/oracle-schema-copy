package com.github;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: pmontgom
 * Date: 4/22/13
 * Time: 5:01 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class AbstractLoadRowsCallback implements RowCallback {
    final Target target;

    public AbstractLoadRowsCallback(Target target) {
        this.target = target;
    }

    List<Object[]> pendingRows = new ArrayList<Object[]>();

    int count = 0;

    @Override
    public void rowArrived(Object[] row) {
        pendingRows.add(row);
        count++;
        if (pendingRows.size() > 10000) {
            flush();
        }
    }

    public void flush() {
        if (pendingRows.size() > 0) {
            Operation operation = createOperation(pendingRows);
            try {
                target.apply(operation);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            pendingRows = new ArrayList<Object[]>();
        }
    }

    abstract Operation createOperation(List<Object[]> rows);
}
