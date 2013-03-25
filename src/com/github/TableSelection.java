package com.github;

import java.util.List;

/**
* Created with IntelliJ IDEA.
* User: pmontgom
* Date: 3/20/13
* Time: 10:49 PM
* To change this template use File | Settings | File Templates.
*/
public class TableSelection {
    String table;
    String column;
    List<Object> values;

    TableSelection(String table, String column, List<Object> values) {
        this.table = table;
        this.column = column;
        this.values = values;
    }
}
