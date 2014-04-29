package test.service;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.springframework.stereotype.Service;

import com.reed.hadoop.hbase.base.HbaseRepositoryImpl;
import com.reed.hadoop.util.JsonUtil;

@Service
public class UserServiceImpl extends HbaseRepositoryImpl implements UserService {

	public TestUser findTByKey(TestUser t) {
		TestUser r = null;
		Result rs = super.findByKey(t);
		if (rs != null) {
			String s = new String(rs.value());
			r = (TestUser) JsonUtil.json2Object(s, TestUser.class);
		}
		return r;
	}

	public List<TestUser> findTByAll(final TestUser t, final String start,
			final String end) {
		List<TestUser> r = null;
		List<Result> rs = super.findAll(t, start, end);
		if (rs != null && rs.size() > 0) {
			r = new ArrayList<TestUser>(rs.size());
			for (Result item : rs) {
				if (item != null) {
					String s = new String(item.value());
					r.add((TestUser) JsonUtil.json2Object(s, TestUser.class));
				}
			}
		}
		return r;
	}
}
