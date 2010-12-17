package com.github;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
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
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oracle.jdbc.OraclePreparedStatement;

public class OracleDumper {
	
	private static final int BATCH_SIZE = 500;

	public static void dropSchema(Connection connection) throws Exception 
	{
		// encountered a problem where could not a table until the domain index
		// was dropped first.  So, do a round of drop indices before dropping
		// everything else.
//		executeFromQuery(connection, "select 'alter table \"'||table_name||'\" drop constraint \"'||constraint_name||'\"' from user_constraints where constraint_type = 'R'");
//		executeFromQuery(connection, "select 'alter table \"'||table_name||'\" drop constraint \"'||constraint_name||'\"' from user_constraints where constraint_type not in ('C', 'R')");
		executeFromQuery(connection, "select 'drop index \"'||index_name||'\" force' from user_indexes where index_type = 'DOMAIN'");
		
		String tableQuery = "select 'drop table \"' ||  table_name ||'\" cascade constraints' from user_tables";
		// domain indexes sometimes create a supporting table
		// which gets dropped when the parent gets dropped.  This will
		// cause an exception when we attempt to drop the underlying
		// table.  To avoid this false error, do the drops twice:  The 
		// first time, ignore exceptions.  The second time, throw exceptions.
		// That way if anything gets dropped twice, it's not a problem, but
		// anything that cannot be dropped is a hard error.
		executeFromQuery(connection, tableQuery, true);
		executeFromQuery(connection, tableQuery, false);
		
		executeFromQuery(connection, "select 'drop synonym \"' ||  synonym_name ||'\"' from user_synonyms");
		executeFromQuery(connection, "select 'drop view \"' ||  view_name ||'\"' from user_views");
		executeFromQuery(connection, "select 'drop function \"' ||  object_name ||'\"' from user_objects where object_type = 'FUNCTION'");
		executeFromQuery(connection, "select 'drop procedure \"' ||  object_name ||'\"' from user_objects where object_type = 'PROCEDURE'");
		executeFromQuery(connection, "select 'drop package \"' ||  object_name ||'\"' from user_objects where object_type = 'PACKAGE'");
		executeFromQuery(connection, "select 'drop type \"' ||  type_name ||  '\" force' from user_types");
		executeFromQuery(connection, "select 'drop sequence \"' ||  sequence_name ||'\"' from user_sequences");
		executeFromQuery(connection, "select 'purge recyclebin' from dual");
	}
	
	private static void executeFromQuery(Connection connection, String query)  throws Exception {
		executeFromQuery(connection, query, false);
	}
	
	private static void executeFromQuery(Connection connection, String query, boolean ignoreException)  throws Exception {
		Statement nonprepared = connection.createStatement();

		ResultSet resultSet = nonprepared.executeQuery(query);
		List<String> statements = new ArrayList<String>();
		while(resultSet.next())
		{
			statements.add(resultSet.getString(1));
		}
		
		for(String stmt : statements)
		{
			printSqlToExecute(stmt);
			try 
			{
				nonprepared.execute(stmt);
			} 
			catch (SQLException ex)
			{
				if(ignoreException)
				{
					System.out.println("(Ignoring exception)");
					ex.printStackTrace();
				}
				else
					throw ex;
			}
		}
	}

	static public void printSqlToExecute(String sql)
	{
		Date d = new Date();
		System.out.println(d+" Executing: "+sql);
	}

	public static void importSchema(FileInputStream fis, Connection dest) throws Exception {
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

	private static int writeObject(OutputStream os, Serializable obj) throws Exception
	{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(obj);
        oos.close();
        
        os.write(baos.toByteArray());
        os.flush();
        
        return baos.size();
	}
	

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

	protected static void performInsert(Connection connection, TableDefinition table, List<Object[]> rows) throws Exception
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
				if(table.isLob[i])
				{
					if(row[i] == null)
					{
						statement.setNull(i+1, Types.VARCHAR);
					}
					else if(row[i] instanceof String)
					{
						((OraclePreparedStatement)statement).setStringForClob(i+1,(String)row[i]);
						//Clob clob = new Clob(new StringReader((String)row[i]));
						//statement.setClob(i+1, clob);
					}
					else
					{
						statement.setObject(i+1, row[i] );
					}
				}
				else
				{
					statement.setObject(i+1, row[i]);
				}
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
	
	protected static void execute(Connection connection, String sql) throws Exception
	{
		PreparedStatement statement = connection.prepareStatement(sql);
		statement.execute();
		statement.close();
	}
	
	protected static String stringFromClob(Clob clob) throws Exception
	{
		if(clob == null)
			return null;
		return clob.getSubString(1L, new Long(clob.length()).intValue()) ;
	}
	
	protected static List<String> extractDDL(Connection connection, String srcSchema, String query, Collection<String> excludedTables) throws Exception {
		Pattern triggerPattern = Pattern.compile("(?s)(.*)ALTER\\s+TRIGGER\\s+\\S+\\s+ENABLE\\s*");

		List<String> statements = new ArrayList<String>();
		PreparedStatement preparedStatement = connection.prepareStatement(query);
		ResultSet resultSet = preparedStatement.executeQuery();

		// find all the columns which identify "tables" which need to exist
		Set<String> tableColumnNames = new HashSet<String>();
		ResultSetMetaData metaData = resultSet.getMetaData();
		for(int i = 0;i<metaData.getColumnCount();i++)
		{
			String columnName = metaData.getColumnName(i+1);
			if(columnName.startsWith("TABLE_NAME"))
				tableColumnNames.add(columnName);
		}

		while(resultSet.next())
		{
			String statement = stringFromClob(resultSet.getClob("DDL"));

			boolean skipRow = false;
			for(String columnName : tableColumnNames)
			{
				String value = resultSet.getString(columnName);
				if(excludedTables.contains(value))
					skipRow = true;
			}
			
			if(skipRow)
				continue;
			
			// a hack to cope with the bad sql produced from triggers.  Triggers result in an extra 
			// ddl statement to enable trigger.  If our execute could handle multiple statments (using 
			// using both kinds of delimiters, we could just execute it
			// but since that's a little tricky, let's just strip out the alter statements.
			while(true)
			{
				Matcher m = triggerPattern.matcher(statement);
				if(!m.matches())
					break;
				
//				System.out.println("Before: "+statement);
				statement = m.group(1);
//				System.out.println("After: "+statement);
			}
			
			// correct the schema name on the object
			// do we want to make this regex more specific?
			statement = statement.replaceAll("\""+srcSchema.toUpperCase()+"\".", "");

			System.out.println("statement to exec: "+statement);
			statements.add(statement);
		}

		preparedStatement.close();
		resultSet.close();
		
		return statements;
	}
	
	protected static void addSqlFromDdl(Target target, Connection connection, String srcSchema, String query, Collection<String> excludedTables) throws Exception
	{
		List<String> statements = extractDDL(connection, srcSchema, query, excludedTables);
		
		Operation operation = new ExecuteSqlList(statements);
		target.apply(operation);
	}

	static class OutputStreamTarget implements Target 
	{
		final OutputStream os;
		
		public OutputStreamTarget(OutputStream os)
		{
			this.os = os;
		}

		public void apply(Operation operation) {
			int len;
			try {
				len = writeObject(os, operation);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			System.out.println("Add sql from ddl: "+len);
		}
	}
	
	private static void preExport(Connection connection) throws Exception
	{
		// configure dbms_metadata export
		execute(connection, "begin "+
	            "dbms_metadata.SET_TRANSFORM_PARAM(dbms_metadata.SESSION_TRANSFORM, 'TABLESPACE', FALSE); "+
	            "dbms_metadata.SET_TRANSFORM_PARAM(dbms_metadata.SESSION_TRANSFORM, 'STORAGE', FALSE); "+
	            "dbms_metadata.SET_TRANSFORM_PARAM(dbms_metadata.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', FALSE); "+
	            "dbms_metadata.SET_TRANSFORM_PARAM(dbms_metadata.SESSION_TRANSFORM, 'REF_CONSTRAINTS', FALSE); "+
	            "end; ");
	}
	
	private static void exportSchemaTables(Target target, Connection connection, String srcSchema, Collection<String> excludedTables) throws Exception
	{
		// ddl for creating tables
		// skip any tables that are actually secondary objects of a domain index
		System.out.println("excluded="+excludedTables);
		for(String tableName : getTableNames(connection))
		{
			if(excludedTables.contains(tableName))
			{
				System.out.println("Skipping table: "+tableName);
				continue;
			}
			else
			{
				System.out.println("exporting : \""+tableName+"\"");
			}
			
			// forgive me for generating sql on the fly...  I really do feel guilty about it. 
			addSqlFromDdl(target, connection, srcSchema, "select dbms_metadata.GET_DDL('TABLE', '"+tableName+"') ddl from dual", excludedTables);
		}
	}
	
	public static void exportSchemaWithData (Target target, Connection connection, String srcSchema, Set<String> excludedTables, Set<String> excludedSequences) throws Exception
	{
		preExport(connection);

		exportSchemaTables(target, connection, srcSchema, excludedTables);
		
		// copy over the data to the target
		addTableInsertOperations(target, connection, excludedTables);
		
		exportSchemaTableDependentObjects(target, connection, srcSchema, excludedTables);

		exportSchemaOtherObjects(target, connection, srcSchema, excludedSequences);
	}

	public static void directCopySchema(final Connection dstConnection, Connection srcConnection, String dstSchema, String srcSchema, Collection<String> excludedTables, Collection<String> excludedSequences) throws Exception
	{
		preExport(srcConnection);

		Target destConnectionTarget = new Target()
		{
			public void apply(Operation operation) throws Exception {
				operation.execute(dstConnection);
			}
		};
		
		exportSchemaTables(destConnectionTarget, srcConnection, srcSchema, excludedTables);

		//copy over the data via selects
		copyTableDataViaInsertSelects(dstConnection, srcConnection, dstSchema, srcSchema, excludedTables);

		exportSchemaTableDependentObjects(destConnectionTarget, srcConnection, srcSchema, excludedTables);

		exportSchemaOtherObjects(destConnectionTarget, srcConnection, srcSchema, excludedSequences);
	}


	/**
	 * @param target
	 * @param connection
	 * @param srcSchema
	 * @throws Exception
	 */
	private static void exportSchemaTableDependentObjects(Target target,
			Connection connection, String srcSchema, Collection<String> excludedTables) throws Exception {
		// when creating indexes, skip any that are part of a primary or unique key constraint
		// because those were created with the table definition.   Also skip any indexes on tables that actually
		// we secondary objects of domain indexes.  Also skip any LOB indexes because those are created with 
		// the table as well.
		String indexQuery = "select TABLE_NAME, dbms_metadata.GET_DDL('INDEX', index_name) ddl from user_indexes i where " +
				"not exists ( select 1 from user_constraints c where c.constraint_type in ('P','U') and c.index_name = i.index_name) " +
				"and not exists ( select 1 from user_objects so where so.secondary = 'Y' and so.object_name = i.index_name and so.object_type = 'INDEX' ) " +
				"and index_type <> 'LOB' ";

		addSqlFromDdl(target, connection, srcSchema, indexQuery, excludedTables);

		addSqlFromDdl(target, connection, srcSchema, "select TABLE_NAME, dbms_metadata.GET_DDL('REF_CONSTRAINT', constraint_name) ddl from user_constraints where constraint_type = 'R'", excludedTables);
	}


	/**
	 * @param target
	 * @param connection
	 * @param srcSchema
	 * @param excludedSequences 
	 * @throws Exception
	 */
	private static void exportSchemaOtherObjects(Target target,
			Connection connection, String srcSchema, Collection<String> excludedSequences) throws Exception {
		
		Collection<String> excludedTables = Collections.EMPTY_SET;
		
		addSqlFromDdl(target, connection, srcSchema, simpleDbmsExtractSql("PROCEDURE"), excludedTables);

		addSqlFromDdl(target, connection, srcSchema, simpleDbmsExtractSql("FUNCTION"), excludedTables);

		addSqlFromDdl(target, connection, srcSchema, simpleDbmsExtractSql("VIEW"), excludedTables);

		addSqlFromDdl(target, connection, srcSchema, simpleDbmsExtractSql("TRIGGER"), excludedTables);

		addSqlFromDdl(target, connection, srcSchema, "select sequence_name table_name, dbms_metadata.GET_DDL('SEQUENCE', sequence_name) ddl from user_sequences", excludedSequences);
	}

	private static String simpleDbmsExtractSql(String objType) {
		return "select dbms_metadata.GET_DDL('"+objType+"', object_name) ddl from user_objects where object_type = '"+objType+"'";
	}

	private static Object[] findColumns(Connection connection, String tablename) throws Exception
	{
		List<String> cols = new ArrayList<String>();
		List<Boolean> isLob = new ArrayList<Boolean>();
		
		String columnQuery = "select COLUMN_NAME, DATA_TYPE from USER_TAB_COLUMNS WHERE TABLE_NAME = ?";
		PreparedStatement statement = connection.prepareStatement(columnQuery);
		statement.setString(1, tablename);
		ResultSet resultSet = statement.executeQuery();
		while(resultSet.next())
		{
			cols.add(resultSet.getString(1));
			String type = resultSet.getString(2);
			isLob.add(type.equals("CLOB") || type.equals("BLOB"));
		}
		resultSet.close();
		statement.close();

		boolean [] isLobArray = new boolean[isLob.size()];
		for(int i=0;i<isLob.size();i++)
			isLobArray[i] = isLob.get(i);
		
		return new Object[]{cols, isLobArray};
	}

	static class TableSpaceUsage {
		final int bytes;
		final int rows;
		final String table;

		/**
		 * @param bytes
		 * @param rows
		 * @param table
		 */
		public TableSpaceUsage(int bytes, int rows, String table) {
			super();
			this.bytes = bytes;
			this.rows = rows;
			this.table = table;
		}
	}
	
	private static List<String> getUncopiableTables(Connection connection) throws Exception
	{
		List<String> names = new ArrayList<String>();

		String query = "select table_name from user_tab_columns where data_type like 'LONG%'";
		PreparedStatement preparedStatement = connection.prepareStatement(query);
		ResultSet resultSet = preparedStatement.executeQuery();
		while(resultSet.next())
		{
			String table = resultSet.getString(1);
			names.add(table);
		}
		resultSet.close();
		preparedStatement.close();
		
		return names;
	}
	
	private static List<String> getTableNames(Connection connection) throws Exception
	{
		List<String> names = new ArrayList<String>();
		
		String selectTableSql = "select object_name table_name " +
		"from user_objects " +
		"where object_type = 'TABLE' and SECONDARY = 'N' " +
		"and object_name not like 'BIN$%' ";
		
		PreparedStatement preparedStatement = connection.prepareStatement(selectTableSql);
		ResultSet resultSet = preparedStatement.executeQuery();

		while(resultSet.next())
		{
			String table = resultSet.getString(1);
			names.add(table);
		}
		preparedStatement.close();
		resultSet.close();
		
		return names;
	}
	
	private static void copyTableDataViaInsertSelects(Connection dstConnection, Connection srcConnection, String dstSchema, String srcSchema, Collection<String> userExcludedTables) throws Exception
	{
		Statement srcStatement = srcConnection.createStatement();
		Statement dstStatement = dstConnection.createStatement();

		Set<String> excludedTables = new HashSet<String>();
		excludedTables.addAll(getUncopiableTables(srcConnection));
		System.out.println("The following tables have 'LONG' columns and therefore cannot be copied: "+excludedTables);
		excludedTables.addAll(userExcludedTables);
		
		for(String table : getTableNames(srcConnection))
		{
			if(excludedTables.contains(table))
			{
				System.out.println("Skipping table: "+table);
				continue;
			}
			
			String grantSql = "grant select on \""+table+"\" to \""+dstSchema+"\"";
			printSqlToExecute(grantSql);
			srcStatement.executeUpdate(grantSql);
			
			String insertSql = "insert into \""+table+"\" select * from \""+srcSchema+"\".\""+table+"\"";
			printSqlToExecute(insertSql);
			dstStatement.executeUpdate(insertSql);
		}
		dstStatement.close();
		srcStatement.close();
	}
	
	private static void addTableInsertOperations(Target target,
			Connection connection, Set<String> excludedTables) throws Exception {

		List<TableSpaceUsage> usage = new ArrayList<TableSpaceUsage>();
		
		for(String table : getTableNames(connection))
		{
			if(excludedTables.contains(table))
			{
				System.out.println("Skipping table: "+table);
				continue;
			}
			
			Object[] tuple = findColumns(connection, table);
			List<String> columns = (List<String>)tuple[0];
			boolean isLob [] = (boolean[]) tuple[1];
			
			TableDefinition tableDefinition = new TableDefinition(table, columns, isLob);
			List<Object[]> rows = exportTable(connection, tableDefinition);
			System.out.println("Exported "+rows.size()+" from "+table);

			Operation operation = new ExecuteTableLoad(tableDefinition, rows);
			target.apply(operation);
		}
		
		Collections.sort(usage, new Comparator<TableSpaceUsage>() {
			public int compare(TableSpaceUsage o1, TableSpaceUsage o2) {
				return -(o1.bytes - o2.bytes);
			}
		});

		System.out.println("Table space consumption:");
		for(int i=0;i<Math.min(usage.size(), 20);i++)
		{
			TableSpaceUsage u = usage.get(i);
			System.out.println(u.table+": "+u.bytes+", "+u.rows);
		}
	}

	public static Properties loadProperties()
	{
		Properties properties = new Properties();
		String propertiesFilename = System.getProperty("com.github.OracleDumper.properties");
		if(propertiesFilename != null)
		{
			FileReader reader;
			try {
				reader = new FileReader(propertiesFilename);
			} catch (FileNotFoundException e) {
				throw new RuntimeException("Could not open: "+propertiesFilename);
			}
			try {
				properties.load(reader);
				reader.close();
			} catch (IOException e) {
				throw new RuntimeException("Could not read: "+propertiesFilename);
			}
		}
		return properties;
	}

	
	
	public static void main(String args[]) throws Exception
	{
		Properties properties = loadProperties();
		String jdbcString = args[1];
		String username = args[2].toUpperCase();
		String password = args[3];

		DriverManager.registerDriver (new oracle.jdbc.driver.OracleDriver());
		
		if(args[0].equals("dropObjects"))
		{
			Connection connection = DriverManager.getConnection(jdbcString, username, password);
			dropSchema(connection);
		}
		else if(args[0].equals("export"))
		{
			String filename = args[4];

			FileOutputStream fos = new FileOutputStream(filename);
			Connection connection = DriverManager.getConnection(jdbcString, username, password);

			Set<String> excludedTables = getExcludedTables(properties, "export.exclude.tables");
			Set<String> excludedSequence = getExcludedTables(properties, "export.exclude.sequences");
			
			exportSchemaWithData(new OutputStreamTarget(fos), connection, username, excludedTables, excludedSequence);
		}
		else if(args[0].equals("import"))
		{
			String filename = args[4];

			FileInputStream fis = new FileInputStream(filename);
			Connection connection = DriverManager.getConnection(jdbcString, username, password);
			importSchema(fis, connection);
		} 
		else if(args[0].equals("copy"))
		{
			String dstSchema = args[2].toUpperCase();
			String dstPassword = args[3];
			String srcSchema = args[4].toUpperCase();
			String srcPassword = args[5];

			Connection dstConnection = DriverManager.getConnection(jdbcString, dstSchema, dstPassword);
			Connection srcConnection = DriverManager.getConnection(jdbcString, srcSchema, srcPassword);

			Set<String> excludedTables = getExcludedTables(properties, "copy.exclude.tables");
			Set<String> excludedSequences = getExcludedTables(properties, "copy.exclude.sequences");
			
			directCopySchema(dstConnection, srcConnection, dstSchema, srcSchema, excludedTables, excludedSequences);
		}
		else
		{
			System.err.println("Usage: dropObjects jdbc_uri username password | export jdbc_uri username password filename | import jdbc_uri username password | copy jdbc_uri dst_username dst_password src_username src_password");
			System.exit(-1);
		}
	}

	/**
	 * @param properties
	 * @return
	 */
	private static Set<String> getExcludedTables(Properties properties, String propertyName) {
		Set<String> excludedTables = new HashSet<String>();
		String excludeString = properties.getProperty(propertyName);
		if(excludeString != null)
		{
			for(String tableName : excludeString.split(","))
			{
				excludedTables.add(tableName.trim().toUpperCase());
			}
		}
		return excludedTables;
	}
}
