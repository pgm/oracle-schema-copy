package com.github;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: pmontgom
 * Date: 4/22/13
 * Time: 5:00 PM
 * To change this template use File | Settings | File Templates.
 */
public class InsertRowsCallback extends AbstractLoadRowsCallback {
    final TableDefinition tableDefinition;

    public InsertRowsCallback(Target target, TableDefinition tableDefinition) {
        super(target);
        this.tableDefinition = tableDefinition;
    }

    Operation createOperation(List<Object[]> rows) {
        return new ExecuteTableLoad(tableDefinition, rows);
    }
}
