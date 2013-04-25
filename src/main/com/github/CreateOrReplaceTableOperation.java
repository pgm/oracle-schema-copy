package com.github;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created with IntelliJ IDEA.
 * User: pmontgom
 * Date: 3/21/13
 * Time: 4:13 PM
 * To change this template use File | Settings | File Templates.
 */
public class CreateOrReplaceTableOperation implements Operation {
    final String createSql;
    final String dropSql;

    public CreateOrReplaceTableOperation(String createSql, String dropSql) {
        this.createSql = createSql;
        this.dropSql = dropSql;
    }

    @Override
    public void execute(Connection connection) {
        try {
            Statement statement = connection.createStatement();
            try {

                // try creating, and if it fails, try dropping and re-creating
                try {
                    statement.execute(createSql);
                } catch (SQLException ex) {
                    statement.execute(dropSql);
                    statement.execute(createSql);
                }

            } finally {
                statement.close();
            }

        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }
}
