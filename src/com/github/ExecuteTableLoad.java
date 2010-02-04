/**
 * 
 */
package com.github;

import java.sql.Connection;
import java.util.List;

import com.github.OracleDumper.TableDefinition;

class ExecuteTableLoad implements Operation
{
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