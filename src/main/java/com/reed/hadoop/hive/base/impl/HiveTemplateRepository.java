package com.reed.hadoop.hive.base.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hive.HiveOperations;
import org.springframework.stereotype.Repository;

import com.reed.hadoop.domain.base.BaseObj;
import com.reed.hadoop.hive.base.HiveRepository;

@Repository(value = "baseHiveTemplate")
public class HiveTemplateRepository implements HiveRepository<BaseObj> {

	@Autowired
	private HiveOperations hiveOperations;

	@Override
	public Long count(BaseObj t) {
		Long r = null;
		if (t != null) {
			r = hiveOperations.queryForLong("select count(*) from "
					+ t.getTableName());
		}
		return r;
	}
}
