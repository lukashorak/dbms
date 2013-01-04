package com.luki.db.proj.model;

public class SimpleCondition {
	public String logic;
	public static final String[] types = { "=", "<", "<=", ">", ">=", "!=",
			"IN", "NOT IN" };
	public String type;

	public String field1Type;
//	public String field1;
	public Field field1;

	public String field2Type;
//	public String field2;
	public Field field2;

}