package com.luki.db.proj.model;

public class SimpleAggregation {
	public static final String[] types = { "COUNT", "SUM", "AVG", "MAX", "MIN" };
	public String type;

	public boolean allField = false;
//	public String field;
	public Field field;
	
	public Integer pos;

	// public Double value;
}