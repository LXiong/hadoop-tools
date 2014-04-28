package com.reed.hadoop.domain.base;

import java.io.Serializable;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

/**
 * base domain
 * 
 * @author reed
 * 
 */
public class BaseObj implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1905530792801140314L;
	/** domain mapping to table name */
	private String tableName;
	/** domain key for hbase and hive */
	private String key;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public BaseObj() {
	}

	public BaseObj(String tableName) {
		super();
		this.tableName = tableName;
	}

	public BaseObj(String tableName, String key) {
		super();
		this.tableName = tableName;
		this.key = key;
	}

	public boolean equals(Object obj) {
		return EqualsBuilder.reflectionEquals(this, obj);
	}

	public int hashCode() {
		return HashCodeBuilder.reflectionHashCode(this);
	}

	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
	}
}
