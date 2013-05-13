package com.github.utils

import java.sql.Connection
import java.sql.DriverManager

/**
 * Created with IntelliJ IDEA.
 * User: ddurkin
 * Date: 5/9/13
 * Time: 1:37 PM
 * To change this template use File | Settings | File Templates.
 */
class SqlUtils {

    /**
     *
     * @param map with keys for url, username, and url
     * @return a Connection for to the db
     */
    static Connection getConnection(Map map) {
        try {
            assert map.url
            assert map.username
            assert map.password
            Class.forName("oracle.jdbc.driver.OracleDriver")
            Connection connection = DriverManager.getConnection(map.url, map.username, map.password);
            connection.setAutoCommit(false);
            return connection;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}


