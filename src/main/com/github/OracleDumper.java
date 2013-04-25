package com.github;

import java.io.*;
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
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import oracle.jdbc.OraclePreparedStatement;

public class OracleDumper {
/*

    public static void dropSchema(Connection connection) throws Exception {
        // encountered a problem where could not a table until the domain index
        // was dropped first.  So, do a round of drop indices before dropping
        // everything else.
//		executeFromQuery(connection, "select 'alter table \"'||table_name||'\" drop constraint \"'||constraint_name||'\"' from user_constraints where constraint_type = 'R'");
//		executeFromQuery(connection, "select 'alter table \"'||table_name||'\" drop constraint \"'||constraint_name||'\"' from user_constraints where constraint_type not in ('C', 'R')");
        executeFromQuery(connection, "select 'drop index \"' ||index_name|| '\"' from user_indexes where index_type = 'DOMAIN'");

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

    private static void executeFromQuery(Connection connection, String query) throws Exception {
        executeFromQuery(connection, query, false);
    }


    public static void exportSchemaWithData(Target target, Connection connection, String srcSchema, Set<String> excludedTables, Set<String> excludedSequences) throws Exception {
        preExport(connection);

        exportSchemaTables(target, connection, srcSchema, excludedTables);

        Set<String> tableNames = new HashSet(getTableNames(connection));
        tableNames.removeAll(excludedTables);

        // copy over the data to the target
        addTableInsertOperations(target, connection, tableNames);

        exportSchemaTableDependentObjects(target, connection, srcSchema, excludedTables);

        exportSchemaOtherObjects(target, connection, srcSchema, excludedSequences);
    }

    public static void directCopySchema(final Connection dstConnection, Connection srcConnection, String dstSchema, String srcSchema, Collection<String> excludedTables, Collection<String> excludedSequences) throws Exception {
        preExport(srcConnection);

        Target destConnectionTarget = new ExecuteTarget(dstConnection);

        exportSchemaTables(destConnectionTarget, srcConnection, srcSchema, excludedTables);

        //copy over the data via selects
        copyTableDataViaInsertSelects(dstConnection, srcConnection, dstSchema, srcSchema, excludedTables);

        exportSchemaTableDependentObjects(destConnectionTarget, srcConnection, srcSchema, excludedTables);

        exportSchemaOtherObjects(destConnectionTarget, srcConnection, srcSchema, excludedSequences);
    }

    public static void copyData(final Connection dstConnection, Connection srcConnection, Collection<String> excludedTables) throws Exception {
        Target destConnectionTarget = new ExecuteTarget(dstConnection);

        Set<String> tableNames = new HashSet(getTableNames(srcConnection));
        tableNames.removeAll(excludedTables);

        addTableInsertOperations(destConnectionTarget, srcConnection, tableNames);
    }

    private static void exportSchemaTableDependentObjects2(Target target,
                                                          Connection connection, String srcSchema, Collection<String> tables) throws Exception {
        // when creating indexes, skip any that are part of a primary or unique key constraint
        // because those were created with the table definition.   Also skip any indexes on tables that actually
        // we secondary objects of domain indexes.  Also skip any LOB indexes because those are created with
        // the table as well.
        String indexQuery = "select TABLE_NAME, dbms_metadata.GET_DDL('INDEX', index_name) ddl from user_indexes i where " +
                "not exists ( select 1 from user_constraints c where c.constraint_type in ('P','U') and c.index_name = i.index_name) " +
                "and not exists ( select 1 from user_objects so where so.secondary = 'Y' and so.object_name = i.index_name and so.object_type = 'INDEX' ) " +
                "and index_type <> 'LOB' ";

        addSqlFromDdl(target, connection, srcSchema, indexQuery);

        addSqlFromDdl(target, connection, srcSchema, "select TABLE_NAME, dbms_metadata.GET_DDL('REF_CONSTRAINT', constraint_name) ddl from user_constraints where constraint_type = 'R'");
    }




    private static List<String> getUncopiableTables(Connection connection) throws Exception {
        List<String> names = new ArrayList<String>();

        String query = "select table_name from user_tab_columns where data_type like 'LONG%'";
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            String table = resultSet.getString(1);
            names.add(table);
        }
        resultSet.close();
        preparedStatement.close();

        return names;
    }


    private static void copyTableDataViaInsertSelects(Connection dstConnection, Connection srcConnection, String dstSchema, String srcSchema, Collection<String> userExcludedTables) throws Exception {
        Statement srcStatement = srcConnection.createStatement();
        Statement dstStatement = dstConnection.createStatement();

        Set<String> excludedTables = new HashSet<String>();
        excludedTables.addAll(getUncopiableTables(srcConnection));
        System.out.println("The following tables have 'LONG' columns and therefore cannot be copied: " + excludedTables);
        excludedTables.addAll(userExcludedTables);

        for (String table : getTableNames(srcConnection)) {
            if (excludedTables.contains(table)) {
                System.out.println("Skipping table: " + table);
                continue;
            }

            String grantSql = "grant select on \"" + table + "\" to \"" + dstSchema + "\"";
            printSqlToExecute(grantSql);
            srcStatement.executeUpdate(grantSql);

            String insertSql = "insert into \"" + table + "\" select * from \"" + srcSchema + "\".\"" + table + "\"";
            printSqlToExecute(insertSql);
            dstStatement.executeUpdate(insertSql);
        }
        dstStatement.close();
        srcStatement.close();
    }


    public static Properties loadProperties() {
        Properties properties = new Properties();
        String propertiesFilename = System.getProperty("com.github.OracleDumper.properties");
        if (propertiesFilename != null) {
            FileReader reader;
            try {
                reader = new FileReader(propertiesFilename);
            } catch (FileNotFoundException e) {
                throw new RuntimeException("Could not open: " + propertiesFilename);
            }
            try {
                properties.load(reader);
                reader.close();
            } catch (IOException e) {
                throw new RuntimeException("Could not read: " + propertiesFilename);
            }
        }
        return properties;
    }


    public static void main(String args[]) throws Exception {
        Properties properties = loadProperties();
        String jdbcString = args[1];
        String username = args[2].toUpperCase();
        String password = args[3];

        DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());

        if (args[0].equals("dropObjects")) {
            Connection connection = DriverManager.getConnection(jdbcString, username, password);
            dropSchema(connection);
        } else if (args[0].equals("export")) {
            String filename = args[4];

            FileOutputStream fos = new FileOutputStream(filename);
            Connection connection = DriverManager.getConnection(jdbcString, username, password);

            Set<String> excludedTables = getExcludedTables(properties, "export.exclude.tables");
            Set<String> excludedSequence = getExcludedTables(properties, "export.exclude.sequences");

            exportSchemaWithData(new OutputStreamTarget(fos), connection, username, excludedTables, excludedSequence);
        } else if (args[0].equals("import")) {
            String filename = args[4];

            GZIPInputStream fis = new GZIPInputStream(new FileInputStream(filename));
            Connection connection = DriverManager.getConnection(jdbcString, username, password);
            importSchema(fis, connection);
        } else if(args[0].equals("importuncompressed")) {
            String filename = args[4];

            FileInputStream fis = new FileInputStream(filename);
            Connection connection = DriverManager.getConnection(jdbcString, username, password);
            importSchema(fis, connection);
        } else if (args[0].equals("copy")) {
            String dstSchema = args[2].toUpperCase();
            String dstPassword = args[3];
            String srcSchema = args[4].toUpperCase();
            String srcPassword = args[5];

            Connection dstConnection = DriverManager.getConnection(jdbcString, dstSchema, dstPassword);
            Connection srcConnection = DriverManager.getConnection(jdbcString, srcSchema, srcPassword);

            Set<String> excludedTables = getExcludedTables(properties, "copy.exclude.tables");
            Set<String> excludedSequences = getExcludedTables(properties, "copy.exclude.sequences");

            directCopySchema(dstConnection, srcConnection, dstSchema, srcSchema, excludedTables, excludedSequences);
        } else if (args[0].equals("exportData")) {
            Connection srcConnection = DriverManager.getConnection(jdbcString, username, password);

            String filename = args[4];

            Collection<String> tables = getTableNames(srcConnection);

            FileOutputStream fos = new FileOutputStream(filename);
            addTableInsertOperations(new OutputStreamTarget(fos), srcConnection, tables);
            fos.close();

        } else if (args[0].equals("copyData")) {
            String srcJdbcString = args[4];
            String srcSchema = args[5].toUpperCase();
            String srcPassword = args[6];

            Connection dstConnection = DriverManager.getConnection(jdbcString, username, password);
            Connection srcConnection = DriverManager.getConnection(srcJdbcString, srcSchema, srcPassword);

            copyData(dstConnection, srcConnection, Collections.EMPTY_LIST);
        } else {
            System.err.println("Usage: dropObjects jdbc_uri username password | export jdbc_uri username password filename | import jdbc_uri username password | copy jdbc_uri dst_username dst_password src_username src_password");
            System.exit(-1);
        }
    }

    static public List<TableSelection> reorderTableSelections(List<String> ordering, List<TableSelection> selections) {
        Map<String, TableSelection> selectionMap = new HashMap<String, TableSelection>();
        for (TableSelection selection : selections) {
            selectionMap.put(selection.table, selection);
        }
        List<TableSelection> result = new ArrayList();
        for (String table : ordering) {
            TableSelection selection = selectionMap.get(table);
            if (selection != null)
                result.add(selection);
        }
        return result;
    }

    private static Set<String> getExcludedTables(Properties properties, String propertyName) {
        Set<String> excludedTables = new HashSet<String>();
        String excludeString = properties.getProperty(propertyName);
        if (excludeString != null) {
            for (String tableName : excludeString.split(",")) {
                excludedTables.add(tableName.trim().toUpperCase());
            }
        }
        return excludedTables;
    }
    */
}
