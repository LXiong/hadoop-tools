package com.reed.hadoop.hive.base;

import java.util.List;
import java.util.Map;

import com.reed.hadoop.domain.base.BaseObj;

/**
 * base hive table repository include base methods
 * 
 * @author reed
 * 
 */
public interface HiveRepository<T extends BaseObj> {
	/**
	 * get count for table
	 * 
	 * @param t
	 * @return
	 */
	Long count(T t);

	/**
	 * DDL or DML hql or sql to execute
	 * 
	 * @param hql
	 * @return
	 */
	int executeHql(String hql);

	/**
	 * Query by sql,return resultSet
	 * 
	 * @param sql
	 * @return
	 */
	List<Map<String, Object>> querySql(String sql);
}
