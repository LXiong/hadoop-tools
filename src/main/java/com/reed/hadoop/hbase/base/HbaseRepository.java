package com.reed.hadoop.hbase.base;

import java.util.List;

import org.apache.hadoop.hbase.client.Result;

import com.reed.hadoop.domain.base.HbaseObj;

/**
 * Repository for Hbase tables
 * 
 * @author reed
 * 
 * @param <T>
 */
public interface HbaseRepository<T extends HbaseObj> {
	/**
	 * using T bean's name as hbase family and fields as columns
	 * 
	 * @param t
	 * @return
	 */
	int createTable(HbaseObj t);

	int dropTable(String tableName);

	/**
	 * HbaseObj指定的表下是否存在指定的family
	 * 
	 * @param t
	 * @return -2，表不存在，-1，family已存在，0，参数错误，1，family不存在
	 */
	int existFamily(HbaseObj t);

	/**
	 * HbaseObj指定的表下添加指定的family
	 * 
	 * @param t
	 * @return -2，表不存在，-1，family已存在，0，参数错误，1，添加成功
	 */
	int addFamily(HbaseObj t);

	/**
	 * HbaseObj指定的表下添加指定的family
	 * 
	 * @param t
	 * @return -2，表不存在，-1，family不存在，0，参数错误，1，删除成功
	 */
	int dropFamily(HbaseObj t);

	/**
	 * 插入记录， Hbase存储格式：Hbase table: T.tableName;Column family:T.family; Hbase
	 * columns:T的全部属性+T.family，T.family列存储T对象（Json）,各属性列存储对应属性值
	 * 
	 * @param t
	 * @return
	 */
	int save(HbaseObj t);

	Result findByKey(HbaseObj t);

	/**
	 * 根据HbaseObj.tableName,HBaseObj.family指定值，查询rowKey从startRow（包括）到endRow（不包括）
	 * 的原始记录
	 * 
	 * @param t
	 * @param startRow
	 * @param endRow
	 * @return
	 */
	List<Result> findAll(HbaseObj t, String startRow, String endRow);

	int deleteByColumn(HbaseObj t, String column);

	int deleteByKey(HbaseObj t);

	T findTByKey(T t);

	/**
	 * 根据T.tableName,T.family指定值，查询rowKey从startRow（包括）到endRow（不包括） 的记录并转换为业务对象T
	 * 
	 * @param t
	 * @param start
	 * @param end
	 * @return
	 */
	List<T> findTByAll(T t, String start, String end);

}
