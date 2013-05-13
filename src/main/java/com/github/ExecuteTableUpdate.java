/**
 *
 */
package com.github;

import java.sql.Connection;
import java.util.List;


class ExecuteTableUpdate implements Operation {
    private static final long serialVersionUID = -143998643301142984L;

    List<Object[]> rows;
    String primaryKey;
    TableDefinition tableDefinition;

    public void execute(Connection connection) {
        CopyUtils.performInsertOrUpdate(connection, primaryKey, tableDefinition, rows);
    }

    public ExecuteTableUpdate(String primaryKey, TableDefinition tableDefinition, List<Object[]> rows2) {
        super();
        this.primaryKey = primaryKey;
        this.rows = rows2;
        this.tableDefinition = tableDefinition;
    }
}