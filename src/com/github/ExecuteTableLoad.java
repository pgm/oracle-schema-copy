/**
 * 
 */
package com.github;

import java.sql.Connection;
import java.util.List;


class ExecuteTableLoad implements Operation
{
	private static final long serialVersionUID = -143998643301142984L;
	
	List<Object[]> rows;
	TableDefinition tableDefinition;
	
	public void execute(Connection connection) throws Exception {
		OracleDumper.performInsert(connection, tableDefinition, rows);
	}

	public ExecuteTableLoad(TableDefinition tableDefinition, List<Object[]> rows2) {
		super();
		this.rows = rows2;
		this.tableDefinition = tableDefinition;
	}
}