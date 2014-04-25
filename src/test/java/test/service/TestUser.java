package test.service;

import com.reed.hadoop.domain.base.HbaseObj;

public class TestUser extends HbaseObj {

	private String name;

	private Long id;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

}
