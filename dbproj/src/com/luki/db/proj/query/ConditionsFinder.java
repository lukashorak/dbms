package com.luki.db.proj.query;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.luki.db.proj.model.Field;
import com.luki.db.proj.model.HavingCondition;
import com.luki.db.proj.model.SimpleCondition;

public class ConditionsFinder {

	public static ArrayList<SimpleCondition> findSimpleCondition(
			String conditions, ArrayList<Field> fields) {
		ArrayList<SimpleCondition> simpleConditions = new ArrayList<SimpleCondition>();

		String[] cArray = conditions.split("(AND|OR)");
		Pattern p = Pattern.compile("(AND|OR)");
		Matcher m = p.matcher(conditions);
		m.find();
		Pattern pAgg = Pattern.compile("(max|min|count|sum|avg)\\[([\\w]*)\\]");

		for (int i = 0; i < cArray.length; i++) {
			String c = cArray[i];
			SimpleCondition sc = new SimpleCondition();

			if (i > 0) {
				sc.logic = m.group();
				m.find();
			} else {
				sc.logic = "AND";
			}
			for (String t : SimpleCondition.types) {
				if (c.contains(t)) {
					sc.type = t;
					String[] f = c.split(t);
					sc.field1 = new Field(f[0].trim().replace("'", ""));
					sc.field2 = new Field(f[1].trim().replace("'", ""));

					Matcher m2 = pAgg.matcher(f[0].trim().replace("'", ""));
					if (m2.find()) {
						if (m2.group(1) != null && m2.group(2) != null) {
							sc.field1.name = m2.group(1);
							sc.field1.aggField = m2.group(2);
							sc.field1.type = "AGG";
						}
					}

					m2 = pAgg.matcher(f[1].trim().replace("'", ""));
					if (m2.find()) {
						if (m2.group(1) != null && m2.group(2) != null) {
							sc.field2.name = m2.group(1);
							sc.field2.aggField = m2.group(2);
							sc.field2.type = "AGG";
						}
					}

					sc.field1Type = "V";
					for (Field field : fields) {
						if (field.equals(sc.field1)) {
							sc.field1 = field;
							sc.field1Type = "C";
							break;
						}
						if ("AGG".equals(field.type)
								&& sc.field1.aggField.equals(field.aggField)
								&& sc.field1.name
										.compareToIgnoreCase(field.name) == 0) {
							sc.field1 = field;
							sc.field1Type = "C";
							break;
						}
					}

					sc.field2Type = "V";
					for (Field field : fields) {
						if (field.equals(sc.field2)) {
							sc.field2 = field;
							sc.field2Type = "C";
							break;
						}
					}
					// if (fields.contains(sc.field1)) {
					// sc.field1Type = "C";
					// } else {
					// sc.field1Type = "V";
					// }

					if (fields.contains(sc.field2)) {
						sc.field2Type = "C";
					} else {
						sc.field2Type = "V";
					}

				}
			}
			simpleConditions.add(sc);
		}
		return simpleConditions;
	}

	@Deprecated
	public static ArrayList<HavingCondition> findHavingCondition(
			String conditions, ArrayList<String> fields) {
		ArrayList<HavingCondition> havingConditions = new ArrayList<HavingCondition>();

		String[] cArray = conditions.split("(AND|OR)");
		Pattern p = Pattern.compile("(AND|OR)");
		Matcher m = p.matcher(conditions);
		m.find();

		for (int i = 0; i < cArray.length; i++) {
			String c = cArray[i];
			HavingCondition hc = new HavingCondition();

			if (i > 0) {
				hc.logic = m.group();
				m.find();
			} else {
				hc.logic = "AND";
			}
			for (String t : HavingCondition.types) {
				if (c.contains(t)) {
					hc.type = t;
					String[] f = c.split(t);
					// hc.field1 = f[0].trim();
					// hc.field2 = f[1].trim();
					hc.field1 = new Field(f[0].trim().replace("'", ""));
					hc.field2 = new Field(f[1].trim().replace("'", ""));

					hc.field1Type = "V";
					if (fields.contains(hc.field1)) {
						hc.field1Type = "C";
					} else {
						// TODO FIXME - Aggregate fields
						// for (HavingCondition)
					}

					if (fields.contains(hc.field2)) {
						hc.field2Type = "C";
					} else {
						hc.field2Type = "V";
					}

				}
			}
			havingConditions.add(hc);
		}
		return havingConditions;
	}
}
