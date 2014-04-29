package com.reed.hadoop.hive.base.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.core.io.ResourceLoader;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
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
	@Autowired
	private JdbcTemplate jdbcTemplate;

	private ResourceLoader resourceLoader;

	@Override
	public void setResourceLoader(ResourceLoader resourceLoader) {
		this.resourceLoader = resourceLoader;
	}

	@Override
	public Long count(BaseObj t) {
		return jdbcOperations.queryForObject(
				"select count(*) from " + t.getTableName(), Long.class);
	}

	@Override
	public int executeHql(final String hql) {
		int r = 0;
		//jdbcTemplate.execute(hql);
		jdbcOperations.execute(hql);
		r = 1;
		return r;
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
