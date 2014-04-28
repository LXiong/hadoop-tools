package com.reed.hadoop.hive.base;

import java.sql.ResultSet;
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

	int executeHql(String hql);

	List<Map<String, Object>> querySql(String sql);
}
