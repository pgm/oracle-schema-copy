package com.github;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: pmontgom
 * Date: 4/5/13
 * Time: 1:30 PM
 * To change this template use File | Settings | File Templates.
 */
public class DeleteByPk implements Operation {
    final String table;
    final String pkColumn;
    final List<Object> keys;

    public DeleteByPk(String table, String pkColumn, List<Object> keys) {
        this.table = table;
        this.pkColumn = pkColumn;
        this.keys = keys;
    }

    @Override
    public void execute(Connection connection) {
        try {
            PreparedStatement statement = connection.prepareStatement("delete from \"" + table + "\" where \"" + pkColumn + "\" = ?");
            try {
                for (Object key : keys) {
                    statement.setObject(1, key);
                    statement.addBatch();
                }
                statement.executeBatch();
            } finally {
                statement.close();
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }
}
