/**
 * 
 */
package com.github;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

class TableDefinition implements Serializable{
	private static final long serialVersionUID = -1597058241468761618L;
	public String name;
	public List<String> columnNames = new ArrayList<String>();
	public TableDefinition(String name, List<String> columnNames) {
		super();
		this.name = name;
		this.columnNames = columnNames;
	}
}