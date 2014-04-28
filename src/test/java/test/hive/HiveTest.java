package test.hive;

/**
 * test hive template
 */
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.hadoop.hive.HiveTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.reed.hadoop.domain.base.BaseObj;
import com.reed.hadoop.hive.base.HiveRepository;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:tools-test.xml")
public class HiveTest {

	private static final Log log = LogFactory.getLog(HiveTest.class);
	@Autowired
	private HiveTemplate template;

	@Autowired
	@Qualifier(value = "baseHiveTemplate")
	private HiveRepository<BaseObj> repository;

	@Autowired
	@Qualifier(value = "baseHiveJdbc")
	private HiveRepository<BaseObj> repositoryJdbc;

	private static final String hql = "CREATE EXTERNAL TABLE hbase_t3(key string,v1 map<string,string>) "
			+ "STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'"
			+ "WITH SERDEPROPERTIES (\"hbase.columns.mapping\" = \":key,TestUser:\")"
			+ "TBLPROPERTIES(\"hbase.table.name\" = \"test1\")";

	@Before
	public final void setUp() {
		List<String> tables = template.query("show tables;");
		if (!tables.isEmpty()) {
			for (String t : tables) {
				log.info(">>>>>>>>>>" + t);
			}
		}
	}

	@After
	public final void clear() {

	}

	@Test
	public final void testCount() {
		BaseObj t = new BaseObj("hbase_t2");
		long r = repository.count(t);
		log.info("Count is = " + r);
		Assert.assertTrue(r > 0);
	}

	@Test
	public final void testCountByJdbc() {
		BaseObj t = new BaseObj("hbase_t2");
		long r = repositoryJdbc.count(t);
		log.info("Count is = " + r);
		Assert.assertTrue(r > 0);
	}

	@Test
	public final void testExecute() {
		int r = repository.executeHql(hql);
		log.info("r is = " + r);
		Assert.assertTrue(r > 0);
	}

	@Test
	public final void testQueryByJdbc() {
		String sql = "select * from hbase_t3";
		List<Map<String, Object>> r = repository.querySql(sql);
		// List<Map<String, Object>> r = repositoryJdbc.querySql(sql);
		log.info("=============>r is = " + r.toString());
		Assert.assertNotNull(r);
	}
}
