package com.github;

import java.io.OutputStream;

/**
* Created with IntelliJ IDEA.
* User: pmontgom
* Date: 3/21/13
* Time: 11:52 AM
* To change this template use File | Settings | File Templates.
*/
public class OutputStreamTarget implements Target {
    final OutputStream os;

    public OutputStreamTarget(OutputStream os) {
        this.os = os;
    }

    public void apply(Operation operation) {
        int len;
        try {
            len = OracleDumper.writeObject(os, operation);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println("Add sql from ddl: " + len);
    }
}
