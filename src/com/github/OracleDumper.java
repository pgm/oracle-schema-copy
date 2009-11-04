package com.github;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OracleDumper {
	
	private static final int BATCH_SIZE = 500;

	static class TableDefinition implements Serializable{
		public String name;
		public List<String> columnNames = new ArrayList<String>();
		public TableDefinition(String name, List<String> columnNames) {
			super();
			this.name = name;
			this.columnNames = columnNames;
		}
	}
	
	public static void dropSchema(Connection connection) throws Exception 
	{
		executeFromQuery(connection, "select 'drop table ' ||  table_name ||  ' cascade constraints' from user_tables");
		executeFromQuery(connection, "select 'drop synonym ' ||  synonym_name from user_synonyms");
		executeFromQuery(connection, "select 'drop view ' ||  view_name from user_views");
		executeFromQuery(connection, "select 'drop function ' ||  object_name from user_objects where object_type = 'FUNCTION'");
		executeFromQuery(connection, "select 'drop procedure ' ||  object_name from user_objects where object_type = 'PROCEDURE'");
		executeFromQuery(connection, "select 'drop package ' ||  object_name from user_objects where object_type = 'PACKAGE'");
		executeFromQuery(connection, "select 'drop type ' ||  type_name ||  ' force' from user_types");
		executeFromQuery(connection, "select 'drop sequence ' ||  sequence_name from user_sequences");
		executeFromQuery(connection, "select 'purge recyclebin' from dual");
	}
	
	
	private static void executeFromQuery(Connection connection, String query)  throws Exception {
		Statement nonprepared = connection.createStatement();

		ResultSet resultSet = nonprepared.executeQuery(query);
		List<String> statements = new ArrayList<String>();
		while(resultSet.next())
		{
			statements.add(resultSet.getString(1));
		}
		
		for(String stmt : statements)
		{
			System.out.println(stmt);
			nonprepared.execute(stmt);
		}
	}



	private static void importSchema(FileInputStream fis, Connection dest) throws Exception {
		while(true)
		{
			ObjectInputStream ios;
			try 
			{
				ios = new ObjectInputStream(fis);
			} 
			catch (EOFException ex)
			{
				break;
			}
			
			Operation operation;
			operation = (Operation)ios.readObject();
			operation.execute(dest);
		}
		dest.commit();
	}


	public static void writeObject(OutputStream os, Serializable obj) throws Exception
	{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        oos.close();
        
        System.out.println("bytes writen: "+baos.size());
        os.write(baos.toByteArray());
        os.flush();
	}
	
//	public static List<TableDefinition> getDefinitions(Connection connection) throws Exception
//	{
//		Map<String, TableDefinition> defs = new HashMap<String, TableDefinition>();
//
//		PreparedStatement statement = connection.prepareStatement("select TABLE_NAME, COLUMN_NAME from USER_TABLES");
//		ResultSet resultSet = statement.executeQuery();
//		while(resultSet.next())
//		{
//			String table = resultSet.getString(1);
//			String column = resultSet.getString(2);
//
//			TableDefinition def = defs.get(table);
//			if(def == null)
//				def = new TableDefinition();
//			def.name = table;
//			def.columnNames.add(column);
//		}
//
//		return new ArrayList<TableDefinition>(defs.values());
//	}
	
	public static List<Object []> exportTable(Connection connection, TableDefinition table) throws Exception 
	{
		List<Object[]> rows = new ArrayList<Object[]>();
		
		
		StringBuilder sb = new StringBuilder();
		sb.append("select ");

		boolean first = true;
		for(String columnName : table.columnNames)
		{
			if(!first)
				sb.append(", ");
			sb.append(columnName);
			first = false;
		}
		sb.append(" from ");

		sb.append("\"");
		sb.append(table.name);
		sb.append("\"");
		
		System.out.println("sql: "+sb.toString());
		PreparedStatement statement = connection.prepareStatement(sb.toString());
		ResultSet resultSet = statement.executeQuery();
		ResultSetMetaData metadata = resultSet.getMetaData();
		while(resultSet.next())
		{
			Object [] row = new Object[table.columnNames.size()];
			
			for(int i=0;i<table.columnNames.size();i++)
			{
				Object value;
				if(metadata.getColumnType(i+1) == Types.CLOB)
				{
					Clob clob = resultSet.getClob(i+1);					
					value = stringFromClob(clob);
				}
				else if (metadata.getColumnType(i+1) == Types.BLOB)
				{
					Blob blob = resultSet.getBlob(i+1);					
					value = byteArrayFromClob(blob);
				}
				else
				{
					value = resultSet.getObject(i+1);
				}
				row[i] = value;
			}
			
			rows.add(row);
		}
		
		return rows;
	}
	
	private static byte[] byteArrayFromClob(Blob blob) throws Exception {
		if (blob == null)
			return null;
		return blob.getBytes(1, (int) blob.length());
	}

	public static void performInsert(Connection connection, TableDefinition table, List<Object[]> rows) throws Exception
	{
		StringBuilder sb = new StringBuilder();
		sb.append("insert into \"");
		sb.append(table.name);
		sb.append("\" (");
		boolean first = true;
		for(String columnName : table.columnNames)
		{
			if(!first)
				sb.append(", ");
			sb.append(columnName);
			first = false;
		}
		sb.append(") values (");
		first = true;
		for(int i=0;i<table.columnNames.size();i++)
		{
			if(!first)
				sb.append(", ");
			sb.append("?");
			first = false;
		}
		sb.append(")");
		System.out.println(""+sb);
		PreparedStatement statement = connection.prepareStatement(sb.toString());
		
		int batchedCount = 0;
		for(Object[] row : rows)
		{
			for(int i=0;i<table.columnNames.size();i++)
			{
				statement.setObject(i+1, row[i]);
			}
			statement.addBatch();
			batchedCount ++;
			
			if(batchedCount > BATCH_SIZE)
			{
				statement.executeBatch();
				batchedCount = 0;
			}
		}
		
		if(batchedCount > 0)
			statement.executeBatch();
		
		statement.close();
	}
	
	public static void execute(Connection connection, String sql) throws Exception
	{
		PreparedStatement statement = connection.prepareStatement(sql);
		statement.execute();
		statement.close();
	}
	
	public static String stringFromClob(Clob clob) throws Exception
	{
		if(clob == null)
			return null;
		return clob.getSubString(1L, new Long(clob.length()).intValue()) ;
	}
	
	public static List<String> extractDDL(Connection connection, String srcSchema, String query) throws Exception {
		Pattern triggerPattern = Pattern.compile("(?s)(.*)ALTER TRIGGER \"[^\"]+\"\\.\"[^\"]+\" ENABLE\\s*");

		List<String> statements = new ArrayList<String>();
		PreparedStatement preparedStatement = connection.prepareStatement(query);
		ResultSet resultSet = preparedStatement.executeQuery();
		while(resultSet.next())
		{
			String statement = stringFromClob(resultSet.getClob(1));

			// correct the schema name on the object
			statement = statement.replaceAll("\""+srcSchema.toUpperCase()+"\".", "");

			// now a hack to cope with the bad sql produced from triggers.  Triggers result in an extra 
			// ddl statement to enable trigger.  If our execute could handle multiple statments (using 
			// using both kinds of delimiters, we could just execute it
			// but since that's a little tricky, let's just strip out the alter statements.
			Matcher m = triggerPattern.matcher(statement);
			if (m.matches()) {
				statement = m.group(1);
			}
		
			statements.add(statement);
		}

		preparedStatement.close();
		resultSet.close();
		
		return statements;
	}
	
	static interface Operation extends Serializable
	{
		public void execute(Connection connection) throws Exception;
	}
	
	static class ExecuteSqlList implements Operation
	{
		List<String> statements;

		public ExecuteSqlList(List<String> statements) {
			super();
			this.statements = statements;
		}

		public void execute(Connection connection) throws Exception {
			Statement statement = connection.createStatement();
			for(String str : statements)
				statement.execute(str);
		}
		
	}
	
	static class ExecuteTableLoad implements Operation
	{
		List<Object[]> rows;
		TableDefinition tableDefinition;
		
		public void execute(Connection connection) throws Exception {
			performInsert(connection, tableDefinition, rows);
		}

		public ExecuteTableLoad(TableDefinition tableDefinition, List<Object[]> rows2) {
			super();
			this.rows = rows2;
			this.tableDefinition = tableDefinition;
		}
	}

	public static void addSqlFromDdl(OutputStream os, Connection connection, String srcSchema, String query) throws Exception
	{
		List<String> statements = extractDDL(connection, srcSchema, query);
		Operation operation = new ExecuteSqlList(statements);
		writeObject(os, operation);
	}
	
	public static void exportSchema (OutputStream os, Connection connection, String srcSchema) throws Exception
	{
		// configure dbms_metadata export
		execute(connection, "begin "+
	            "dbms_metadata.SET_TRANSFORM_PARAM(dbms_metadata.SESSION_TRANSFORM, 'TABLESPACE', FALSE); "+
	            "dbms_metadata.SET_TRANSFORM_PARAM(dbms_metadata.SESSION_TRANSFORM, 'STORAGE', FALSE); "+
	            "dbms_metadata.SET_TRANSFORM_PARAM(dbms_metadata.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', FALSE); "+
	            "dbms_metadata.SET_TRANSFORM_PARAM(dbms_metadata.SESSION_TRANSFORM, 'REF_CONSTRAINTS', FALSE); "+
	            "end; ");

		// ddl for creating tables
		// skip any tables that are actually secondary objects of a domain index
		addSqlFromDdl(os, connection, srcSchema, "select dbms_metadata.GET_DDL('TABLE', object_name) ddl from user_objects t where t.object_type = 'TABLE' and SECONDARY = 'N' and t.object_name not like 'BIN$%'");
		
		addTableInsertOperations(os, connection);

		addSqlFromDdl(os, connection, srcSchema, "select dbms_metadata.GET_DDL('SEQUENCE', sequence_name) ddl from user_sequences");

		// when creating indexes, skip any that are part of a primary or unique key constraint
		// because those were created with the table definition.   Also skip any indexes on tables that actually
		// we secondary objects of domain indexes.  Also skip any LOB indexes because those are created with 
		// the table as well.
		String indexQuery = "select dbms_metadata.GET_DDL('INDEX', index_name) ddl from user_indexes i where " +
				"not exists ( select 1 from user_constraints c where c.constraint_type in ('P','U') and c.index_name = i.index_name) " +
				"and not exists ( select 1 from user_objects so where so.secondary = 'Y' and so.object_name = i.index_name and so.object_type = 'INDEX' ) " +
				"and index_type <> 'LOB' ";

		addSqlFromDdl(os, connection, srcSchema, indexQuery);

		addSqlFromDdl(os, connection, srcSchema, "select dbms_metadata.GET_DDL('REF_CONSTRAINT', constraint_name) ddl from user_constraints where constraint_type = 'R'");

		addSqlFromDdl(os, connection, srcSchema, simpleDbmsExtractSql("PROCEDURE"));

		addSqlFromDdl(os, connection, srcSchema, simpleDbmsExtractSql("FUNCTION"));

		addSqlFromDdl(os, connection, srcSchema, simpleDbmsExtractSql("VIEW"));

		addSqlFromDdl(os, connection, srcSchema, simpleDbmsExtractSql("TRIGGER"));
	}

	private static String simpleDbmsExtractSql(String objType) {
		return "select dbms_metadata.GET_DDL('"+objType+"', object_name) ddl from user_objects where object_type = '"+objType+"'";
	}

	private static List<String> findColumns(Connection connection, String tablename) throws Exception
	{
		List<String> cols = new ArrayList<String>();
		
		String columnQuery = "select COLUMN_NAME from USER_TAB_COLUMNS WHERE TABLE_NAME = ?";
		PreparedStatement statement = connection.prepareStatement(columnQuery);
		statement.setString(1, tablename);
		ResultSet resultSet = statement.executeQuery();
		while(resultSet.next())
		{
			cols.add(resultSet.getString(1));
		}
		resultSet.close();
		statement.close();
		return cols;
	}
	
	private static void addTableInsertOperations(OutputStream os,
			Connection connection) throws Exception {

		String selectTableSql = "select object_name table_name " +
				"from user_objects " +
				"where object_type = 'TABLE' and SECONDARY = 'N' " +
				"and object_name not like 'BIN$%' ";
		

		PreparedStatement preparedStatement = connection.prepareStatement(selectTableSql);
		ResultSet resultSet = preparedStatement.executeQuery();
		
		while(resultSet.next())
		{
			String table = resultSet.getString(1);
			
			TableDefinition tableDefinition = new TableDefinition(table, findColumns(connection, table));
			List<Object[]> rows = exportTable(connection, tableDefinition);
			System.out.println("Exported "+rows.size()+" from "+table);

			Operation operation = new ExecuteTableLoad(tableDefinition, rows);
			writeObject(os, operation);
		}
		
		preparedStatement.close();
		resultSet.close();
	}
}
