package com.github;

import oracle.jdbc.OraclePreparedStatement;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: pmontgom
 * Date: 3/24/13
 * Time: 10:41 PM
 * To change this template use File | Settings | File Templates.
 */
public class CopyUtils {
    private static final int BATCH_SIZE = 500;
    static Pattern LINK_PATTERN = Pattern.compile("(\\w+)->(\\w+)\\.(\\w+)");

    static void deleteSelections(Target target, List<TableSelection> selections) {
        for (TableSelection selection : selections) {
            deleteSelection(selection, target);
        }
    }

    static void deleteSelection(TableSelection selection, Target target) {
        target.apply(new DeleteByPk(selection.table, selection.column, selection.values));
    }

    static void copySelections(Connection connection, List<TableSelection> selections, Target target) {
        for (TableSelection selection : selections) {
            copySelection(connection, selection, target);
        }
    }

    static void copySelection(Connection connection, TableSelection selection, Target target) {
        TableDefinition tableDefinition = getTableDefinition(connection, selection.table);
        InsertRowsCallback callback = new InsertRowsCallback(target, tableDefinition);
        exportTable(connection, tableDefinition, selection.column, selection.values, callback);
        callback.flush();
        if (callback.count != selection.values.size()) {
            throw new RuntimeException("Internal error: expected " + selection.values.size() + " rows selected from " + selection.table + " but got " + callback.count);
        }
    }


    static List<TableSelection> selectAlongPath(Connection connection, String[] paths, String startingTable, List<Object> startingIds) {
        Set<String> tables = extractTablesFromPaths(paths);
        Map<String, String> pks = getPrimaryKeys(connection, tables);
        List<ForeignKeyRelationship> fks = constructFkDefs(pks, paths);

        List<TableSelection> selections = walkLinked(connection, fks, startingTable, startingIds, pks);
        return selections;
    }

    static Set<String> extractTablesFromPaths(String[] paths) {
        Set<String> tables = new HashSet<String>();

        for (String path : paths) {
            Matcher m = LINK_PATTERN.matcher(path);
            if (m.matches()) {
                tables.add(m.group(1));
                tables.add(m.group(2));
            } else {
                throw new RuntimeException("Could not parse path: " + path);
            }
        }

        return tables;
    }

    static String getParentTableFromPath(String path) {
        Matcher m = LINK_PATTERN.matcher(path);
        if (m.matches()) {
            return m.group(1);
        } else {
            throw new RuntimeException("Could not parse path: " + path);
        }
    }

    static List<ForeignKeyRelationship> constructFkDefs(Map<String, String> pks, String[] paths) {
        List<ForeignKeyRelationship> results = new ArrayList<ForeignKeyRelationship>();

        for (String path : paths) {
            Matcher m = LINK_PATTERN.matcher(path);
            if (m.matches()) {
                results.add(new ForeignKeyRelationship(m.group(1), pks.get(m.group(1)), m.group(2), m.group(3)));
            } else {
                throw new RuntimeException("Could not parse path: " + path);
            }
        }

        return results;
    }


    protected static String stringFromClob(Clob clob) throws SQLException {
        if (clob == null)
            return null;
        return clob.getSubString(1L, new Long(clob.length()).intValue());
    }

    protected static String stripOutTriggerDDL(String statement) {
        Pattern triggerPattern = Pattern.compile("(?s)(.*)ALTER\\s+TRIGGER\\s+\\S+\\s+ENABLE\\s*");

        // a hack to cope with the bad sql produced from triggers.  Triggers result in an extra
        // ddl statement to enable trigger.  If our execute could handle multiple statments (using
        // using both kinds of delimiters, we could just execute it
        // but since that's a little tricky, let's just strip out the alter statements.
        while (true) {
            Matcher m = triggerPattern.matcher(statement);
            if (!m.matches())
                break;

//				System.out.println("Before: "+statement);
            statement = m.group(1);
//				System.out.println("After: "+statement);
        }

        return statement;
    }

    protected static String dropSchemaName(String srcSchema, String statement) {
        // correct the schema name on the object
        // do we want to make this regex more specific?
        statement = statement.replaceAll("\"" + srcSchema.toUpperCase() + "\".", "");
        return statement;
    }

    protected static List<String> extractDDL(Connection connection, String srcSchema, String query) {

        List<String> statements = new ArrayList<String>();
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            try {
                ResultSet resultSet = preparedStatement.executeQuery();
                try {
                    // find all the columns which identify "tables" which need to exist
                    Set<String> tableColumnNames = new HashSet<String>();
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    for (int i = 0; i < metaData.getColumnCount(); i++) {
                        String columnName = metaData.getColumnName(i + 1);
                        if (columnName.startsWith("TABLE_NAME"))
                            tableColumnNames.add(columnName);
                    }

                    while (resultSet.next()) {
                        String statement = stringFromClob(resultSet.getClob("DDL"));

//                boolean skipRow = false;
//                for (String columnName : tableColumnNames) {
//                    String value = resultSet.getString(columnName);
//                    if (excludedTables.contains(value))
//                        skipRow = true;
//                }
//
//                if (skipRow)
//                    continue;

                        statement = dropSchemaName(srcSchema, statement);
                        System.out.println("statement to exec: " + statement);
                        statements.add(statement);
                    }
                } finally {
                    resultSet.close();
                }
            } finally {
                preparedStatement.close();
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }

        return statements;
    }

    protected static void addSqlFromDdl(Target target, Connection connection, String srcSchema, String query) {
        List<String> statements = extractDDL(connection, srcSchema, query);

        Operation operation = new ExecuteSqlList(statements);
        target.apply(operation);
    }

    private static void preExport(Connection connection) {
        // configure dbms_metadata export
        execute(connection, "begin " +
                "dbms_metadata.SET_TRANSFORM_PARAM(dbms_metadata.SESSION_TRANSFORM, 'TABLESPACE', FALSE); " +
                "dbms_metadata.SET_TRANSFORM_PARAM(dbms_metadata.SESSION_TRANSFORM, 'STORAGE', FALSE); " +
                "dbms_metadata.SET_TRANSFORM_PARAM(dbms_metadata.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', FALSE); " +
                "dbms_metadata.SET_TRANSFORM_PARAM(dbms_metadata.SESSION_TRANSFORM, 'REF_CONSTRAINTS', FALSE); " +
                "end; ");
    }

    private static void exportSchemaTablesList(Target target, Connection connection, String srcSchema, Collection<String> tableNames) {
        // ddl for creating tables
        // skip any tables that are actually secondary objects of a domain index
        for (String tableName : tableNames) {
            System.out.println("exporting : \"" + tableName + "\"");

            // forgive me for generating sql on the fly...  I really do feel guilty about it.
            addSqlFromDdl(target, connection, srcSchema, "select dbms_metadata.GET_DDL('TABLE', '" + tableName + "') ddl from dual");
        }
    }

    private static void exportPackages(Target target, Connection connection, String srcSchema) {
        Collection<String> packageNames = getObjectNames(connection, "object_type = 'PACKAGE'");
        exportPackages(target, connection, srcSchema, packageNames);
    }

    private static void exportPackages(Target target, Connection connection, String srcSchema, Collection<String> packageNames) {
        for (String packageName : packageNames) {
            addSqlFromDdl(target, connection, srcSchema, "select dbms_metadata.GET_DDL('PACKAGE_SPEC', '" + packageName + "') ddl from dual");
            Collection<String> packageBodys = getObjectNames(connection, "object_type = 'PACKAGE BODY' and object_name='" + packageName + "'");
            for (String packageBody : packageBodys) {
                System.out.println("Printing Package: " + packageBody);
                addSqlFromDdl(target, connection, srcSchema, "select dbms_metadata.GET_DDL('PACKAGE_BODY', '" + packageBody + "') ddl from dual");
            }
        }
    }

    private static void exportTriggers(Target target, Connection connection, String srcSchema, Collection<String> triggerNames) {
        for (String triggerName : triggerNames) {
            addSqlFromDdl(target, connection, srcSchema, "select dbms_metadata.GET_DDL('TRIGGER', '" + triggerName + "') ddl from dual");
        }
    }

    private static void exportProcedures(Target target, Connection connection, String srcSchema, Collection<String> procedureNames) {
        for (String procName : procedureNames) {
            addSqlFromDdl(target, connection, srcSchema, "select dbms_metadata.GET_DDL('PROCEDURE', '" + procName + "') ddl from dual");
        }
    }

    private static void exportSequences(Target target, Connection connection, String srcSchema, Collection<String> sequences) {
        for (String sequenceName : sequences) {
            addSqlFromDdl(target, connection, srcSchema, "select sequence_name table_name, dbms_metadata.GET_DDL('SEQUENCE', '" + sequenceName + "') ddl from dual");
        }
    }

    private static void exportProcedures(Target target, Connection connection, String srcSchema) {
        addSqlFromDdl(target, connection, srcSchema, simpleDbmsExtractSql("PROCEDURE"));
    }

    private static void exportFunctions(Target target, Connection connection, String srcSchema) {
        addSqlFromDdl(target, connection, srcSchema, simpleDbmsExtractSql("FUNCTION"));
    }

    private static void exportViews(Target target, Connection connection, String srcSchema) {
        addSqlFromDdl(target, connection, srcSchema, simpleDbmsExtractSql("VIEW"));
    }

    private static void exportTriggers(Target target, Connection connection, String srcSchema) {
        List<String> statements = extractDDL(connection, srcSchema, simpleDbmsExtractSql("TRIGGER"));
        List<String> finalStatements = new ArrayList();

        for(String statement : statements) {
            finalStatements.add(stripOutTriggerDDL(statement));
        }

        Operation operation = new ExecuteSqlList(finalStatements);
        target.apply(operation);
    }

    private static void exportSequences(Target target, Connection connection, String srcSchema) {
        addSqlFromDdl(target, connection, srcSchema, simpleDbmsExtractSql("SEQUENCE"));
    }

    private static String simpleDbmsExtractSql(String objType) {
        return simpleDbmsExtractSql(objType, objType);
    }

    private static String simpleDbmsExtractSql(String objType, String ddlType) {
        return "select dbms_metadata.GET_DDL('" + ddlType + "', object_name) ddl from user_objects where object_type = '" + objType + "'";
    }


    public static List<String> getTableNames(Connection connection) {
        return getObjectNames(connection, "object_type = 'TABLE' and SECONDARY = 'N' and object_name not like 'BIN$%' ");
    }

    public static List<String> getObjectNames(Connection connection, String whereClause) {
        List<String> names = new ArrayList<String>();

        String selectTableSql = "select object_name " +
                "from user_objects " +
                "where "+whereClause;

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(selectTableSql);
            try {
                ResultSet resultSet = preparedStatement.executeQuery();
                try {

                    while (resultSet.next()) {
                        String table = resultSet.getString(1);
                        names.add(table);
                    }
                } finally {
                    resultSet.close();
                }
            } finally {
                preparedStatement.close();
            }

            return names;
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static void executeFromQuery(Connection connection, String query, boolean ignoreException) {
        try {
            Statement nonprepared = connection.createStatement();
            try {

                ResultSet resultSet = nonprepared.executeQuery(query);
                try {
                    List<String> statements = new ArrayList<String>();
                    while (resultSet.next()) {
                        statements.add(resultSet.getString(1));
                    }

                    for (String stmt : statements) {
                        printSqlToExecute(stmt);
                        try {
                            nonprepared.execute(stmt);
                        } catch (SQLException ex) {
                            if (ignoreException) {
                                System.out.println("(Ignoring exception)");
                                ex.printStackTrace();
                            } else
                                throw ex;
                        }
                    }
                } finally {
                    resultSet.close();
                }
            } finally {
                nonprepared.close();
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    static public void printSqlToExecute(String sql) {
        Date d = new Date();
        System.out.println(d + " Executing: " + sql);
    }

    public static void importSchema(InputStream fis, Connection dest) {
        try {
            while (true) {
                ObjectInputStream ios;
                try {
                    ios = new ObjectInputStream(fis);
                } catch (EOFException ex) {
                    break;
                }

                Operation operation;
                operation = (Operation) ios.readObject();
                operation.execute(dest);
            }
            dest.commit();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static int writeObject(OutputStream os, Serializable obj) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(obj);
            oos.close();

            os.write(baos.toByteArray());
            os.flush();

            return baos.size();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static Map<String, String> getPrimaryKeys(Connection connection, Collection<String> tables) {
        Map<String, String> result = new HashMap();
        String query = "select ucc.table_name, ucc.column_name, ucc.position \n" +
                "from user_constraints uc join user_cons_columns ucc on uc.constraint_name = ucc.constraint_name \n" +
                "where uc.constraint_type = 'P' and uc.constraint_name not like 'BIN$%'";
        try {
            Statement statement = connection.createStatement();
            try {
                ResultSet resultSet = statement.executeQuery(query);
                try {
                    while (resultSet.next()) {
                        String table = resultSet.getString(1);
                        if (!tables.contains(table)) {
                            continue;
                        }
                        String primaryKeyColumn = resultSet.getString(2);
                        int columnPosition = resultSet.getInt(3);
                        if (columnPosition != 1) {
                            throw new RuntimeException("Table " + table + " has more then one column as its primary key");
                        }
                        result.put(table, primaryKeyColumn);
                    }
                } finally {
                    resultSet.close();
                }
            } finally {
                statement.close();
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        return result;
    }

    public static List<ForeignKeyRelationship> getFkRelationships(Connection connection) {
        String query = "select uccc.constraint_name, uccc.table_name, uccc.column_name, uccp.table_name, uccp.column_name from USER_CONSTRAINTS uc join user_cons_columns uccc on uc.constraint_name = uccc.constraint_name \n" +
                "join user_cons_columns uccp on uc.r_constraint_name = uccp.constraint_name\n" +
                "where uc.constraint_type = 'R'\n" +
                "and uccc.position = uccp.position " +
                "and uc.constraint_name not like 'BIN$%'";

        List<ForeignKeyRelationship> result = new ArrayList();
        try {
            Statement statement = connection.createStatement();
            try {
                ResultSet resultSet = statement.executeQuery(query);
                try {
                    while (resultSet.next()) {
                        String name = resultSet.getString(1);
                        String parentTable = resultSet.getString(2);
                        String parentColumn = resultSet.getString(3);
                        String childTable = resultSet.getString(4);
                        String childColumn = resultSet.getString(5);
                        result.add(new ForeignKeyRelationship(name, parentTable, parentColumn, childTable, childColumn));
                    }
                } finally {
                    resultSet.close();
                }
            } finally {
                statement.close();
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        return result;
    }

    public static List<ForeignKeyRelationship> filterRelationships(List<ForeignKeyRelationship> relationships, Collection<String> exclusions) {
        List<ForeignKeyRelationship> results = new ArrayList<ForeignKeyRelationship>();
        for (ForeignKeyRelationship relationship : relationships) {
            if (!(exclusions.contains(relationship.childTable + "." + relationship.childColumn + "=" + relationship.parentTable + "." + relationship.parentColumn) ||
                    exclusions.contains(relationship.parentTable + "." + relationship.parentColumn + "=" + relationship.childTable + "." + relationship.childColumn))) {
                results.add(relationship);
            }
        }
        return results;
    }

    public static MultiMap<String, ForeignKeyRelationship> getFkRelationshipsByTable(Collection<ForeignKeyRelationship> relationships) {
        MultiMap<String, ForeignKeyRelationship> map = new MultiMap();
        for (ForeignKeyRelationship relationship : relationships) {
            map.add(relationship.childTable, relationship);
            map.add(relationship.parentTable, relationship);
        }
        return map;
    }

    private static List<Object> findLinkedRowsBatch(Connection connection, List<Object> parentIds, String childTable, String childColumn, String childPk) {
        if (parentIds.size() == 0)
            return Collections.EMPTY_LIST;

        StringBuilder questionMarks = new StringBuilder();
        for (int i = 0; i < parentIds.size(); i++) {
            if (i > 0) {
                questionMarks.append(", ");
            }
            questionMarks.append("?");
        }

        List<Object> result = new ArrayList<Object>();
        String query = "SELECT " + childPk + " FROM " + childTable + " c WHERE " + childColumn + " IN (" + questionMarks + ")";
        try {
            PreparedStatement statement = connection.prepareStatement(query);
            try {
                for (int i = 0; i < parentIds.size(); i++) {
                    statement.setObject(i + 1, parentIds.get(i));
                }
                ResultSet resultSet = statement.executeQuery();
                try {
                    while (resultSet.next()) {
                        result.add(resultSet.getObject(1));
                    }
                } finally {
                    resultSet.close();
                }
            } finally {
                statement.close();
            }
        } catch (SQLException ex) {
            throw new RuntimeException("Exception executing: " + query, ex);
        }
        return result;
    }

    private static <T> List<List<T>> partition(List<T> objs, int size) {
        List<List<T>> result = new ArrayList<List<T>>();
        for (int i = 0; i < objs.size(); i += size) {
            result.add(objs.subList(i, Math.min(i + size, objs.size())));
        }
        return result;
    }

    public static List<Object> findLinkedRows(Connection connection, List<Object> parentIds, String childTable, String childColumn, String childPk) {
        List<Object> results = new ArrayList<Object>();
        for (List<Object> parentIdsBatch : partition(parentIds, BATCH_SIZE)) {
            results.addAll(findLinkedRowsBatch(connection, parentIdsBatch, childTable, childColumn, childPk));
        }
        return results;
    }

    public static List<TableSelection> walkLinked(Connection connection, List<ForeignKeyRelationship> links, String table, List<Object> ids, Map<String, String> pks) {
        Map<String, List<Object>> idsByTable = new HashMap();
        idsByTable.put(table, ids);

        List<TableSelection> selections = new ArrayList<TableSelection>();
        selections.add(new TableSelection(table, pks.get(table), ids));

        selections.addAll(walkLinked(connection, links, idsByTable, pks));

        return selections;
    }

    public static List<TableSelection> walkLinked(Connection connection,
                                                  List<ForeignKeyRelationship> links,
                                                  Map<String, List<Object>> idsByTable,
                                                  Map<String, String> pks) {

        List<TableSelection> selections = new ArrayList<TableSelection>();

        for (ForeignKeyRelationship relationship : links) {
            List<Object> ids = idsByTable.get(relationship.parentTable);
            if (ids == null) {
                throw new RuntimeException("Could not find path to " + relationship.parentTable);
            }
            String currentColumn = relationship.parentColumn;
            String nextTable = relationship.childTable;
            String nextColumn = relationship.childColumn;
            String nextTablePk = pks.get(nextTable);

            if (nextTablePk == null) {
                throw new RuntimeException("no PK for " + nextTable + ".  Perhaps this table doesn't exist?");
            }

            List<Object> childIds = findLinkedRows(connection, ids, nextTable, nextColumn, nextTablePk);
            idsByTable.put(nextTable, childIds);

            if (childIds.size() > 0)
                selections.add(new TableSelection(nextTable, nextTablePk, childIds));
        }

        return selections;
    }

    public static List<String> orderTableDependencies(List<ForeignKeyRelationship> relationships) {
        // let ordering = []
        // construct a list of (table, dependancies)
        // for each rec where deps == {}, add table name to ordering and remove rec.table from all pairs
        // repeat.  If no table is without dependancies is found, throw an error because implies a cycle
        MultiMap<String, String> dependencies = new MultiMap<String, String>();
        Set<String> tables = new HashSet<String>();
        List<String> ordered = new ArrayList<String>();

        for (ForeignKeyRelationship relationship : relationships) {
            if (!relationship.childTable.equals(relationship.parentTable))
                dependencies.add(relationship.childTable, relationship.parentTable);

            tables.add(relationship.childTable);
            tables.add(relationship.parentTable);
        }

        while (dependencies.size() > 0) {
            List<String> next = new ArrayList();

            // find tables with no remaining unmet dependencies
            for (String table : tables) {
                if (dependencies.get(table).size() == 0) {
                    next.add(table);
                }
            }

            if (next.size() == 0) {
                throw new RuntimeException("Could not make forward progress.  Suspected cycle in " + dependencies);
            }

            ordered.addAll(next);
            tables.removeAll(next);

            // remove these tables from the unsatisfied dependency lists
            for (String table : next) {
                for (String key : new ArrayList<String>(dependencies.keySet())) {
                    dependencies.remove(key, table);
                }
            }
        }

        return ordered;
    }

    public static void exportTable(Connection connection, TableDefinition table, String criteriaColumn, List<Object> ids, RowCallback callback) {
        for (List<Object> batch : partition(ids, BATCH_SIZE)) {
            exportTableBatch(connection, table, criteriaColumn, batch, callback);
        }
    }

    private static void exportTableBatch(Connection connection, TableDefinition table, String criteriaColumn, List<Object> ids, RowCallback callback) {
        StringBuilder sb = new StringBuilder();
        sb.append("select ");

        generateColumnList(table, sb);
        sb.append(" from ");

        sb.append("\"");
        sb.append(table.name);
        sb.append("\" where ");
        sb.append(criteriaColumn);
        sb.append(" in (");
        boolean first = true;
        for (int i = 0; i < ids.size(); i++) {
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append("?");
        }
        sb.append(")");

        System.out.println("sql: " + sb.toString());

        try {
            PreparedStatement statement = connection.prepareStatement(sb.toString());
            try {
                for (int i = 0; i < ids.size(); i++) {
                    statement.setObject(i + 1, ids.get(i));
                }
                ResultSet resultSet = statement.executeQuery();
                try {
                    readRows(table, resultSet, callback);
                    return;
                } finally {
                    resultSet.close();
                }
            } finally {
                statement.close();
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static void generateColumnList(TableDefinition table, StringBuilder sb) {
        boolean first = true;
        for (String columnName : table.columnNames) {
            if (!first)
                sb.append(", ");
            sb.append(columnName);
            first = false;
        }
    }

    public static void exportTable(Connection connection, TableDefinition table, RowCallback rowCallback) {
        StringBuilder sb = new StringBuilder();

        sb.append("select ");
        generateColumnList(table, sb);
        sb.append(" from ");
        sb.append("\"");
        sb.append(table.name);
        sb.append("\"");

        System.out.println("sql: " + sb.toString());
        try {
            PreparedStatement statement = connection.prepareStatement(sb.toString());
            try {
                ResultSet resultSet = statement.executeQuery();
                try {

                    List<Object[]> rows = new ArrayList<Object[]>();
                    readRows(table, resultSet, rowCallback);
                } finally {
                    resultSet.close();
                }
            } finally {
                statement.close();
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static void readRows(TableDefinition table, ResultSet resultSet, RowCallback callback) throws SQLException {
        ResultSetMetaData metadata = resultSet.getMetaData();
        while (resultSet.next()) {
            Object[] row = new Object[table.columnNames.size()];

            for (int i = 0; i < table.columnNames.size(); i++) {
                Object value;
                if (metadata.getColumnType(i + 1) == Types.CLOB) {
                    Clob clob = resultSet.getClob(i + 1);
                    value = stringFromClob(clob);
                } else if (metadata.getColumnType(i + 1) == Types.BLOB) {
                    Blob blob = resultSet.getBlob(i + 1);
                    value = byteArrayFromClob(blob);
                } else {
                    value = resultSet.getObject(i + 1);
                }
                row[i] = value;
            }

            callback.rowArrived(row);
        }
    }

    private static byte[] byteArrayFromClob(Blob blob) throws SQLException {
        if (blob == null)
            return null;
        return blob.getBytes(1, (int) blob.length());
    }

    protected static void performInsertOrUpdate(Connection connection, String primaryKey, TableDefinition table, List<Object[]> rows) {
        List<List<Object[]>> batches = partition(rows, BATCH_SIZE);
        for (List<Object[]> batch : batches) {
            performInsertOrUpdateBatch(connection, primaryKey, table, batch);
        }
    }

    protected static void performInsertOrUpdateBatch(Connection connection, String primaryKey, TableDefinition table, List<Object[]> rows) {
        System.out.println("updating " + rows.size() + " rows");
        List<Object[]> rowsToInsert = new ArrayList<Object[]>();
        try {
            PreparedStatement statement = connection.prepareStatement(generateUpdateStatement(primaryKey, table));
            try {
                int pkColIndex = table.columnNames.indexOf(primaryKey);
                TableDefinition withoutPk = new TableDefinition(table.name, removeIndex(pkColIndex, table.columnNames), removeIndex(pkColIndex, table.isLob));

                for (Object[] row : rows) {
                    bindRowToStatement(withoutPk, statement, removeIndex(pkColIndex, row));
                    // pk is at the end of the statement
                    statement.setObject(table.columnNames.size(), row[pkColIndex]);
                    int rowsUpdated = statement.executeUpdate();

                    if (rowsUpdated == 0) {
                        rowsToInsert.add(row);
                    } else if (rowsUpdated != 1) {
                        throw new RuntimeException("Was the wrong column given as the primary key?  Updated " + rowsUpdated + " rows with single update");
                    }

                }
            } finally {
                statement.close();
            }

            if (rowsToInsert.size() > 0)
                performInsert(connection, table, rowsToInsert);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static Object[] removeIndex(int index, Object[] elements) {
        Object[] result = new Object[elements.length - 1];
        if (index != 0)
            System.arraycopy(elements, 0, result, 0, index - 1);
        if (index != elements.length - 1)
            System.arraycopy(elements, index + 1, result, index, elements.length - index - 1);
        return result;
    }

    private static boolean[] removeIndex(int index, boolean[] elements) {
        boolean[] result = new boolean[elements.length - 1];
        if (index != 0)
            System.arraycopy(elements, 0, result, 0, index - 1);
        if (index != elements.length - 1)
            System.arraycopy(elements, index + 1, result, index, elements.length - index - 1);
        return result;
    }

    private static <T> List<T> removeIndex(int index, List<T> elements) {
        List<T> result = new ArrayList(elements);
        result.remove(index);
        return result;
    }

    protected static void performInsert(Connection connection, TableDefinition table, List<Object[]> rows) {
        System.out.println("inserting " + rows.size() + " rows");

        StringBuilder sb = generateInsertStatement(table);
        System.out.println("" + sb);
        try {
            PreparedStatement statement = connection.prepareStatement(sb.toString());
            try {

                int batchedCount = 0;
                for (Object[] row : rows) {
                    bindRowToStatement(table, statement, row);
                    statement.addBatch();
                    batchedCount++;

                    if (batchedCount > BATCH_SIZE) {
                        statement.executeBatch();
                        batchedCount = 0;
                    }
                }

                if (batchedCount > 0)
                    statement.executeBatch();
            } finally {
                statement.close();
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static void bindRowToStatement(TableDefinition table, PreparedStatement statement, Object[] row) throws SQLException {
        for (int i = 0; i < table.columnNames.size(); i++) {
            if (table.isLob[i]) {
                if (row[i] == null) {
                    statement.setNull(i + 1, Types.VARCHAR);
                } else if (row[i] instanceof String) {
                    ((OraclePreparedStatement) statement).setStringForClob(i + 1, (String) row[i]);
                } else {
                    statement.setObject(i + 1, row[i]);
                }
            } else {
                statement.setObject(i + 1, row[i]);
            }
        }
    }

    private static String generateUpdateStatement(String primaryKey, TableDefinition table) {
        StringBuilder sb = new StringBuilder();
        sb.append("update \"");
        sb.append(table.name);
        sb.append("\" set ");


        boolean first = true;
        for (String columnName : table.columnNames) {
            if (columnName.equals(primaryKey))
                continue;

            if (!first)
                sb.append(", ");
            sb.append(columnName);
            sb.append("=?");
            first = false;
        }
        sb.append(" where ");
        sb.append(primaryKey);
        sb.append("=?");
        return sb.toString();
    }

    private static StringBuilder generateInsertStatement(TableDefinition table) {
        StringBuilder sb = new StringBuilder();
        sb.append("insert into \"");
        sb.append(table.name);
        sb.append("\" (");
        generateColumnList(table, sb);
        sb.append(") values (");
        boolean first = true;
        for (int i = 0; i < table.columnNames.size(); i++) {
            if (!first)
                sb.append(", ");
            sb.append("?");
            first = false;
        }
        sb.append(")");
        return sb;
    }

    protected static void execute(Connection connection, String sql) {
        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            try {
                statement.execute();
            } finally {
                statement.close();
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static void addTableInsertOperations(Target target,
                                                Connection connection, Collection<String> tableNames) {
        for (String table : tableNames) {
            TableDefinition tableDefinition = getTableDefinition(connection, table);
            InsertRowsCallback callback = new InsertRowsCallback(target, tableDefinition);
            exportTable(connection, tableDefinition, callback);
            callback.flush();
            System.out.println("Exported " + callback.count + " from " + table);

        }
    }

    public static void addTableUpdateOperations(Target target,
                                                Connection connection, Collection<String> tableNames) {
        Map<String, String> pkByTable = getPrimaryKeys(connection, tableNames);
        for (String table : tableNames) {
            TableDefinition tableDefinition = getTableDefinition(connection, table);
            UpdateRowsCallback callback = new UpdateRowsCallback(target, pkByTable.get(table), tableDefinition);
            exportTable(connection, tableDefinition, callback);
            callback.flush();
            System.out.println("Exported " + callback.count + " from " + table);
        }
    }

    public static TableDefinition getTableDefinition(Connection connection, String table) {
        Object[] tuple = findColumns(connection, table);
        List<String> columns = (List<String>) tuple[0];
        boolean isLob[] = (boolean[]) tuple[1];

        return new TableDefinition(table, columns, isLob);
    }

    private static Object[] findColumns(Connection connection, String tablename) {
        try {
            List<String> cols = new ArrayList<String>();
            List<Boolean> isLob = new ArrayList<Boolean>();

            String columnQuery = "select COLUMN_NAME, DATA_TYPE from USER_TAB_COLUMNS WHERE TABLE_NAME = ?";
            PreparedStatement statement = connection.prepareStatement(columnQuery);
            statement.setString(1, tablename);
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                cols.add(resultSet.getString(1));
                String type = resultSet.getString(2);
                isLob.add(type.equals("CLOB") || type.equals("BLOB"));
            }
            resultSet.close();
            statement.close();

            boolean[] isLobArray = new boolean[isLob.size()];
            for (int i = 0; i < isLob.size(); i++)
                isLobArray[i] = isLob.get(i);

            return new Object[]{cols, isLobArray};
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static void exportAll(Connection connection, Target target, String srcSchema, Collection<String> schemaTables, Collection<String> dataTables) {
        preExport(connection);

        Set<String> tableNames = new HashSet(schemaTables);
        exportSchemaTablesList(target, connection, srcSchema, tableNames);

        // copy over the data to the target
        addTableInsertOperations(target, connection, dataTables);

        exportSchemaTableDependentObjects(target, connection, srcSchema);

        exportSchemaOtherObjects(target, connection, srcSchema);

    }

    private static void exportSchemaTableDependentObjects(Target target,
                                                          Connection connection, String srcSchema) {
        // when creating indexes, skip any that are part of a primary or unique key constraint
        // because those were created with the table definition.   Also skip any indexes on tables that actually
        // we secondary objects of domain indexes.  Also skip any LOB indexes because those are created with
        // the table as well.
        String indexQuery = "select TABLE_NAME, dbms_metadata.GET_DDL('INDEX', index_name) ddl from user_indexes i where " +
                "not exists ( select 1 from user_constraints c where c.constraint_type in ('P','U') and c.index_name = i.index_name) " +
                "and not exists ( select 1 from user_objects so where so.secondary = 'Y' and so.object_name = i.index_name and so.object_type = 'INDEX' ) " +
                "and index_type <> 'LOB' ";

        addSqlFromDdl(target, connection, srcSchema, indexQuery);
        addSqlFromDdl(target, connection, srcSchema, "select dbms_metadata.GET_DDL('REF_CONSTRAINT', constraint_name) ddl from user_constraints where constraint_type = 'R'");
    }

    private static void exportSchemaOtherObjects(Target target,
                                                 Connection connection, String srcSchema) {

        exportProcedures(target, connection, srcSchema);

        exportFunctions(target, connection, srcSchema);

        exportViews(target, connection, srcSchema);

        exportTriggers(target, connection, srcSchema);

        exportSequences(target, connection, srcSchema);

        exportPackages(target, connection, srcSchema);
    }

}
