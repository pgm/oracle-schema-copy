package com.github;

import java.sql.Connection;

/**
 * Created with IntelliJ IDEA.
 * User: pmontgom
 * Date: 3/21/13
 * Time: 11:54 AM
 * To change this template use File | Settings | File Templates.
 */
public class ExecuteTarget implements Target {
    final Connection dstConnection;

    public void apply(Operation operation) {
        operation.execute(dstConnection);
    }

    public ExecuteTarget(Connection dstConnection) {
        this.dstConnection = dstConnection;
    }

    @Override
    public void close() {
        try {
            dstConnection.commit();
            dstConnection.close();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
