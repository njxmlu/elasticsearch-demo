package com.iflytek.es.curd;

public class Sxtdept {

	private Integer id;
	private String name;
	private String function;

	public Sxtdept() {
		super();
	}

	public Sxtdept(Integer id, String name, String function) {
		super();
		this.id = id;
		this.name = name;
		this.function = function;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getFunction() {
		return function;
	}

	public void setFunction(String function) {
		this.function = function;
	}
}