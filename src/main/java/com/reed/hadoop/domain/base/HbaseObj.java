package com.reed.hadoop.domain.base;

/**
 * Hbase base domain
 * 
 * @author reed
 * 
 */
public class HbaseObj extends BaseObj {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1668528941739175924L;
	/** Hbase family */
	private String family = this.getClass().getSimpleName();

	// /** Hbase column */
	// private String column;

	public HbaseObj() {
	}

	public HbaseObj(String tableName, String key, String family) {
		super(tableName, key);
		if (family != null) {
			this.family = family;
		}
		// this.column = column;
	}

	public String getFamily() {
		return family;
	}

	public void setFamily(String family) {
		this.family = family;
	}

	// public String getColumn() {
	// return column;
	// }
	//
	// public void setColumn(String column) {
	// this.column = column;
	// }

}
