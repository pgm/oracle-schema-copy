package com.github;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: pmontgom
 * Date: 4/22/13
 * Time: 4:59 PM
 * To change this template use File | Settings | File Templates.
 */
public class UpdateRowsCallback extends AbstractLoadRowsCallback {
    final String pkName;
    final TableDefinition tableDefinition;

    public UpdateRowsCallback(Target target, String primaryKey, TableDefinition tableDefinition) {
        super(target);
        this.pkName = primaryKey;
        this.tableDefinition = tableDefinition;
    }

    Operation createOperation(List<Object[]> rows) {
        return new ExecuteTableUpdate(pkName, tableDefinition, rows);
    }

}
