/**
 * 
 */
package com.github;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

class TableDefinition implements Serializable{
	private static final long serialVersionUID = -1597058491468761619L;
	public String name;
	public List<String> columnNames = new ArrayList<String>();
	public boolean [] isLob;
	public TableDefinition(String name, List<String> columnNames, boolean [] isLob) {
		super();
		this.name = name;
		this.columnNames = columnNames;
		this.isLob = isLob;
	}
}