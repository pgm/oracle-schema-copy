/**
 * 
 */
package com.github;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

class ExecuteSqlList implements Operation
{
	private static final long serialVersionUID = 1963554217334290052L;

	List<String> statements;

	public ExecuteSqlList(List<String> statements) {
		super();
		this.statements = statements;
	}

	public void execute(Connection connection) throws Exception {
		Statement statement = connection.createStatement();
		for(String str : statements)
		{
			try 
			{
				OracleDumper.printSqlToExecute(str);
				statement.execute(str);
			} 
			catch (Exception ex)
			{
				throw new RuntimeException("Could not execute: "+str, ex);
			}
		}
	}
}