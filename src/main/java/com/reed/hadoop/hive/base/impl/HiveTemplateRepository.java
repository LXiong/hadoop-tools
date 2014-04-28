package com.reed.hadoop.hive.base.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.service.HiveClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hive.HiveClientCallback;
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

	@Override
	public int executeHql(final String hql) {
		int r = 0;
		if (StringUtils.isNotBlank(hql)) {
			r = hiveOperations.execute(new HiveClientCallback<Integer>() {
				@Override
				public Integer doInHive(HiveClient hiveClient) throws Exception {
					hiveClient.execute(hql);
					return 1;
				}
			});
		}
		return r;
	}

	@Override
	public List<Map<String, Object>> querySql(String sql) {
		List<Map<String, Object>> r = null;
		if (StringUtils.isNotBlank(sql)) {
			List<String> rs = hiveOperations.query(sql);
			if (rs != null && rs.size() > 0) {
				r = new ArrayList<Map<String, Object>>(1);
				Map<String, Object> m = new HashMap<String, Object>(rs.size());
				for (String s : rs) {
					if (StringUtils.isNotBlank(s)) {
						m.put(s, null);
					}
				}
				r.add(m);
			}
		}
		return r;
	}
}
