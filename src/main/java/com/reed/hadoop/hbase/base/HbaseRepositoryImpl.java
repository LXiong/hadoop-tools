package com.reed.hadoop.hbase.base;

import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseOperations;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.data.hadoop.hbase.TableCallback;
import org.springframework.stereotype.Repository;
import com.reed.hadoop.domain.base.HbaseObj;

import com.reed.hadoop.util.JsonUtil;

@Repository
public abstract class HbaseRepositoryImpl {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(HbaseRepositoryImpl.class);

	@Autowired
	private HbaseTemplate hBaseTemplate;

	@Autowired
	private HbaseOperations hbaseOperations;

	@Autowired
	private HBaseAdmin hBaseAdmin;

	@SuppressWarnings("deprecation")
	// @Override
	public int createTable(final HbaseObj t) {
		int r = 0;
		try {
			if (t != null && StringUtils.isNotBlank(t.getTableName())
					&& StringUtils.isNotBlank(t.getFamily())) {
				if (!hBaseAdmin.tableExists(t.getTableName())) {
					String family = t.getClass().getSimpleName();
					Set<String> columns = getFieldsByObj(t.getClass());
					if (columns != null && columns.size() > 0) {
						HTableDescriptor td = new HTableDescriptor(
								t.getTableName());
						td.addFamily(new HColumnDescriptor(Bytes
								.toBytes(family)));
						hBaseAdmin.createTable(td);
						r = 1;
					}
				} else {
					r = -1;
				}
			}
		} catch (IOException ex) {
			LOGGER.error(
					"========>create Hbase table error:obj is{},msg is {}", t,
					ex.getMessage());
		}
		return r;
	}

	// @Override
	public int dropTable(final String tableName) {
		int r = 0;
		if (StringUtils.isNotBlank(tableName)) {
			r = hBaseTemplate.execute(tableName, new TableCallback<Integer>() {
				public Integer doInTable(HTableInterface hti) throws Throwable {
					if (hBaseAdmin.tableExists(tableName)) {
						hBaseAdmin.disableTable(tableName);
						hBaseAdmin.deleteTable(tableName);
						return 1;
					} else {
						return -1;
					}
				}
			});
		}
		return r;
	}

	// @Override
	public int existFamily(final HbaseObj t) {
		int r = 0;
		try {
			if (t != null && StringUtils.isNotBlank(t.getTableName())
					&& StringUtils.isNotBlank(t.getFamily())) {
				if (hBaseAdmin.tableExists(t.getTableName())) {
					r = hBaseTemplate.execute(t.getTableName(),
							new TableCallback<Integer>() {
								public Integer doInTable(HTableInterface hti)
										throws Throwable {
									HTableDescriptor td = hBaseAdmin
											.getTableDescriptor(Bytes.toBytes(t
													.getTableName()));
									if (td.hasFamily(Bytes.toBytes(t
											.getFamily()))) {
										return -1;
									} else {
										return 1;
									}
								}
							});
				} else {
					return -2;
				}
			}
		} catch (IOException ex) {
			LOGGER.error("============>check Family exist error:{}",
					ex.getMessage());
		}
		return r;
	}

	// @Override
	public int addFamily(final HbaseObj t) {
		int r = 0;
		if (t != null && StringUtils.isNotBlank(t.getTableName())
				&& StringUtils.isNotBlank(t.getFamily())) {
			r = hBaseTemplate.execute(t.getTableName(),
					new TableCallback<Integer>() {
						public Integer doInTable(HTableInterface hti)
								throws Throwable {
							int isexist = existFamily(t);
							if (isexist == 1) {
								HColumnDescriptor cd = new HColumnDescriptor(t
										.getFamily());
								hBaseAdmin.addColumn(t.getTableName(), cd);
								return 1;
							} else {
								return isexist;
							}
						}
					});
		}
		return r;
	}

	// @Override
	public int dropFamily(final HbaseObj t) {
		int r = 0;
		if (t != null && StringUtils.isNotBlank(t.getTableName())
				&& StringUtils.isNotBlank(t.getFamily())) {
			r = hBaseTemplate.execute(t.getTableName(),
					new TableCallback<Integer>() {
						public Integer doInTable(HTableInterface hti)
								throws Throwable {
							int isexist = existFamily(t);
							if (isexist == -1) {
								hBaseAdmin.deleteColumn(t.getTableName(),
										t.getFamily());
								return 1;
							} else {
								return isexist != 1 ? isexist : -1;
							}
						}
					});
		}
		return r;
	}

	// @Override
	public int save(final HbaseObj t) {
		int r = 0;
		if (t != null) {
			r = (Integer) hBaseTemplate.execute(t.getTableName(),
					new TableCallback<Object>() {
						public Object doInTable(HTableInterface hti)
								throws Throwable {
							byte[] k = Bytes.toBytes(t.getKey());
							Put p = new Put(k);
							makeUpPut(p, t.getFamily(), t);
							hti.put(p);
							return 1;
						}
					});
		}
		return r;
	}

	// @Override
	public Result findByKey(final HbaseObj t) {
		Result r = null;
		if (t != null && StringUtils.isNotBlank(t.getTableName())
				&& StringUtils.isNotBlank(t.getFamily())
				&& StringUtils.isNotBlank(t.getKey())) {
			r = hBaseTemplate.execute(t.getTableName(),
					new TableCallback<Result>() {
						public Result doInTable(HTableInterface hti)
								throws Throwable {
							byte[] f = Bytes.toBytes(t.getFamily());
							byte[] k = Bytes.toBytes(t.getKey());
							Get g = new Get(k);
							g.addColumn(f, f);
							Result rt = hti.get(g);
							return rt;
							// String s = new String(rt.value());
							// return (HbaseObj) JsonUtil.json2Object(s,
							// HbaseObj.class);
						}
					});
		}
		return r;
	}

	// @Override
	public List<Result> findAll(final HbaseObj t, final Integer start,
			final Integer end) {
		List<Result> r = null;
		if (t != null && StringUtils.isNotBlank(t.getTableName())
				&& StringUtils.isNotBlank(t.getFamily())) {
			r = (List<Result>) hBaseTemplate.find(t.getTableName(),
					t.getFamily(), new RowMapper<Result>() {
						public Result mapRow(Result result, int rowNum)
								throws Exception {
							return result;
							// String s = new String(result.value());
							// return (HbaseObj) JsonUtil.json2Object(s,
							// HbaseObj.class);
						}
					});
		}
		return r;
	}

	// @Override
	public int deleteByColumn(final HbaseObj t, final String column) {
		int r = 0;
		if (t != null && StringUtils.isNotBlank(t.getTableName())
				&& StringUtils.isNotBlank(t.getFamily())) {
			r = (Integer) hBaseTemplate.execute(t.getTableName(),
					new TableCallback<Object>() {
						public Object doInTable(HTableInterface hti)
								throws Throwable {
							byte[] f = Bytes.toBytes(t.getFamily());
							byte[] k = Bytes.toBytes(t.getKey());
							byte[] c = Bytes.toBytes(column);
							Delete delete = new Delete(k);
							delete.deleteColumns(f, c);
							hti.delete(delete);
							return 1;
						}
					});
		}
		return r;
	}

	// @Override
	public int deleteByKey(final HbaseObj t) {
		int r = 0;
		if (t != null && StringUtils.isNotBlank(t.getTableName())
				&& StringUtils.isNotBlank(t.getFamily())
				&& StringUtils.isNotBlank(t.getKey())) {
			r = (Integer) hBaseTemplate.execute(t.getTableName(),
					new TableCallback<Object>() {
						public Object doInTable(HTableInterface hti)
								throws Throwable {
							byte[] f = Bytes.toBytes(t.getFamily());
							byte[] k = Bytes.toBytes(t.getKey());

							Delete delete = new Delete(k);
							hti.delete(delete);
							return 1;
						}
					});
		}
		return r;
	}

	public Set<String> getFieldsByObj(Class t) {
		Set<String> r = null;
		if (t != null) {
			Field[] fs = t.getDeclaredFields();
			if (fs != null && fs.length > 0) {
				r = new HashSet<String>(fs.length);
				for (Field f : fs) {
					if (f != null) {
						r.add(f.getName());
					}
				}
			}
			Field[] superfs = t.getSuperclass().getDeclaredFields();
			if (superfs != null && superfs.length > 0) {
				r.addAll(getFieldsByObj(t.getSuperclass()));
			}
		}
		return r;
	}

	public Map<String, Object> getObjByReflect(Object t) {
		Map<String, Object> r = null;
		Set<String> fs = getFieldsByObj(t.getClass());
		if (fs != null && fs.size() > 0) {
			r = new HashMap<String, Object>(fs.size());
			for (String f : fs) {
				if (StringUtils.isNotBlank(f) && !f.equals("serialVersionUID")) {
					try {
						PropertyDescriptor pd = new PropertyDescriptor(f,
								t.getClass());
						Method getMethod = pd.getReadMethod();// 获得get方法
						Object v = getMethod.invoke(t);// 执行get方法返回一个Object
						r.put(f, v);
					} catch (Exception ex) {
						LOGGER.error(
								"=============>get getObjByReflect error:{}",
								ex.getMessage());
						continue;
					}
				}
			}
		}

		return r;
	}

	public void makeUpPut(Put p, String family, Object t) {
		if (p != null && t != null) {
			byte[] f = Bytes.toBytes(family);
			p.add(f, f, Bytes.toBytes(JsonUtil.toJson(t)));
			Map<String, Object> m = getObjByReflect(t);
			if (m != null && !m.isEmpty()) {
				for (Map.Entry<String, Object> item : m.entrySet()) {
					if (item != null && item.getValue() != null) {
						byte[] q = Bytes.toBytes(item.getKey());
						byte[] v = Bytes.toBytes(item.getValue() != null ? item
								.getValue().toString() : null);
						p.add(f, q, v);
					}
				}
			}
		}
	}

}
