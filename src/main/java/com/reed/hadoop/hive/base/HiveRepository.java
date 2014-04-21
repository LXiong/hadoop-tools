package com.reed.hadoop.hive.base;

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
}
