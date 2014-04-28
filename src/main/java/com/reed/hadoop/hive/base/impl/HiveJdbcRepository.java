package com.reed.hadoop.hive.base.impl;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.core.io.ResourceLoader;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.ResultSetExtractor;
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

	@Override
	public int executeHql(String hql) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public List<Map<String, Object>> querySql(final String sql) {
		List<Map<String, Object>> r = null;
		if (StringUtils.isNotBlank(sql)) {
			r = jdbcOperations.queryForList(sql);
		}
		return r;
	}
}
