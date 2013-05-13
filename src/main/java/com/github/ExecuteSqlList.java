/**
 *
 */
package com.github;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

class ExecuteSqlList implements Operation {
    private static final long serialVersionUID = 1963554217334290052L;

    List<String> statements;

    public ExecuteSqlList(List<String> statements) {
        super();
        this.statements = statements;
    }

    public void execute(Connection connection) {
        try {
            Statement statement = connection.createStatement();
            try {
                for (String str : statements) {
                    try {
                        CopyUtils.printSqlToExecute(str);
                        statement.execute(str);
                    } catch (Exception ex) {
                        throw new RuntimeException("Could not execute: " + str, ex);
                    }
                }
            } finally {
                statement.close();
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }
}