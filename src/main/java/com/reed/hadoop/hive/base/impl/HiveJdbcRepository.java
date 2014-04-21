package com.reed.hadoop.hive.base.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.core.io.ResourceLoader;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.stereotype.Repository;

import com.reed.hadoop.domain.base.BaseObj;
import com.reed.hadoop.hive.base.HiveRepository;

/**
 * using JDBC to connect hive
 * 
 * @author reed
 * 
 */
@Repository(value = "baseHiveJdbc")
public class HiveJdbcRepository implements HiveRepository<BaseObj>,
		ResourceLoaderAware {
	@Autowired
	private JdbcOperations jdbcOperations;
	private ResourceLoader resourceLoader;

	@Override
	public void setResourceLoader(ResourceLoader resourceLoader) {
		this.resourceLoader = resourceLoader;
	}

	@Override
	public Long count(BaseObj t) {
		return jdbcOperations.queryForLong("select count(*) from "
				+ t.getTableName());
	}
}
