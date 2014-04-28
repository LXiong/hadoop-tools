package test.hbase;

/**
 * test hbase template
 */
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import test.service.TestUser;
import test.service.UserService;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:tools-test.xml")
public class HbaseTest {

	private static final Log log = LogFactory.getLog(HbaseTest.class);
	@Autowired
	private HbaseTemplate hBaseTemplate;
	@Autowired
	private UserService hbaseRepository;

	// private static HbaseObj t;

	private static TestUser t;

	@Before
	public void setUp() {
		// t = new HbaseObj("test1", "k1_" + System.currentTimeMillis(), null);
		t = new TestUser();
		t.setTableName("test1");
		t.setKey("u1_" + System.currentTimeMillis());
		t.setName("uuuu");
		t.setId(1l);
		hbaseRepository.createTable(t);
		hbaseRepository.addFamily(t);
		int r1 = hbaseRepository.save(t);
		Assert.assertTrue(r1 == 1);
		log.info("=============>"
				+ hBaseTemplate.find(t.getTableName(), t.getFamily(),
						new RowMapper<Object>() {
							public Object mapRow(Result result, int rowNum)
									throws Exception {
								return result.toString();
							}
						}));
	}

	@After
	public final void clear() {
//		if (hbaseRepository.existFamily(t) > -2) {
//			int r2 = hbaseRepository.dropTable(t.getTableName());
//			Assert.assertTrue(r2 > 0);
//		}
	}

	@Test
	public final void testCreate() {
		int r1 = hbaseRepository.createTable(t);
		Assert.assertTrue(r1 == -1);
	}

	@Test
	public final void testDrop() {
		int r2 = hbaseRepository.dropTable(t.getTableName());
		Assert.assertTrue(r2 > 0);
	}

	@Test
	public final void testfindByKey() {
		TestUser r = hbaseRepository.findTByKey(t);
		log.info("=============>result is = " + r.toString());
		Assert.assertTrue(r != null);
	}

	@Test
	public final void testfind() {
		List<TestUser> r = hbaseRepository.findTByAll(t, null, null);
		log.info("=============>result is = " + r.toString());
		Assert.assertTrue(r != null);
	}

	@Test
	public final void testDelByKey() {
		List<TestUser> r = hbaseRepository.findTByAll(t, null, null);
		log.info("=============>result is = " + r.toString());
		// int r2 = hbaseRepository.deleteByColumn(t,);
		int r2 = hbaseRepository.deleteByKey(t);
		Assert.assertTrue(r2 == 1);
	}

	@Test
	public final void testDelByColumn() {
		List<TestUser> r = hbaseRepository.findTByAll(t, null, null);
		log.info("=============>result is = " + r.toString());
		int r2 = hbaseRepository.deleteByColumn(t, "key");
		Assert.assertTrue(r2 == 1);
	}
}
