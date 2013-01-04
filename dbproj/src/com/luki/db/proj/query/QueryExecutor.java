package com.luki.db.proj.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import com.luki.db.proj.model.Field;
import com.luki.db.proj.model.Row;
import com.luki.db.proj.model.SimpleAggregation;
import com.luki.db.proj.model.SimpleCondition;

public class QueryExecutor {

	public QueryExecutor() {

	}

	public SelectQuery sq;

	public void executeWithShow(SelectQuery sq) throws QueryException {
		ArrayList<Row> rows = this.execute(sq);

		// this.printRowResult2(rows, true);

		this.printWithOrder(rows, sq.fields, true);
	}

	public String executeSubQuery(SelectQuery sq) throws QueryException {
		ArrayList<Row> rows = this.execute(sq);

		StringBuffer sb = new StringBuffer();
		sb.append("[");
		Iterator<Row> iRow = rows.iterator();
		while (iRow.hasNext()) {
			Row r = iRow.next();
			Iterator<String> it = r.fields.values().iterator();
			while (it.hasNext()) {
				sb.append(it.next());
			}
			if (iRow.hasNext()) {
				sb.append(",");
			}
		}

		sb.append("]");

		return sb.toString();
	}

	public ArrayList<Row> execute(SelectQuery sq) throws QueryException {
		this.sq = sq;

		ArrayList<Row> rows = new ArrayList<Row>();

		// Check for join and fill rows with data
		if (sq.tables.size() == 1) {
			rows.addAll(sq.tables.get(0).rows);
		} else if (sq.tables.size() > 1) {
			rows = this.join();
		}

		if (sq.groupBy) {
			HashMap<String, ArrayList<Row>> groups = new HashMap<String, ArrayList<Row>>();

			// TODO - More GroupBy fields
			Field f = sq.groupByFields.get(0);
			
			// Filter result
						rows = this.filter(rows);
			// for (String f : sq.groupByFields) {

			for (Row r : rows) {
				String v = r.fields.get(f);

				if (groups.containsKey(v)) {
					ArrayList<Row> g = groups.get(v);
					g.add(r);
					groups.put(v, g);
				} else {
					ArrayList<Row> g = new ArrayList<Row>();
					g.add(r);
					groups.put(v, g);
				}
			}

			// }

			ArrayList<Row> shrinkGroups = new ArrayList<Row>();
			for (Entry<String, ArrayList<Row>> e : groups.entrySet()) {
				ArrayList<Row> shrinkGroup = e.getValue();
				if (sq.simpleAggregations.size() > 0) {
					Row agg = this.aggregateResult(shrinkGroup);
					// rows = this.aggregationJoin(rows, agg);
					shrinkGroup = this.aggregationShrink(shrinkGroup, agg);
				}

				if (sq.havingSimpleConditions.size() > 0) {

					Iterator<Row> itRow = shrinkGroup.iterator();
					while (itRow.hasNext()) {
						Row r = itRow.next();

						if (this.filterRowHavingConditions(r)) {
							itRow.remove();
						}

					}
				}
				// ArrayList<Row> shrinkGroup = this.groupShrink(e.getValue());

				// Filter Fields
				for (Row r : shrinkGroup) {
					r = this.filterFields(r);
				}

				shrinkGroups.addAll(shrinkGroup);
			}

			return shrinkGroups;
			// this.printRowResult(shrinkGroups);

		} else {

			if (sq.simpleAggregations.size() > 0) {
				Row agg = this.aggregateResult(rows);
				// rows = this.aggregationJoin(rows, agg);
				rows = this.aggregationShrink(rows, agg);
			}

			// Filter result
			rows = this.filter(rows);

			return rows;
			// Print output
			// this.printRowResult(rows);
		}

	}

	public Row aggregateResult(ArrayList<Row> rows) throws QueryException {
		Row agg = new Row();

		for (SimpleAggregation sa : sq.simpleAggregations) {
			if (sa.type.equals("COUNT")) {

				// FIXME
				Field f = new Field();
				f.name = "COUNT";
				f.type = "AGG_COUNT_" + sa.field;
				agg.fields.put(f, "" + rows.size());
				// r.fields.put("COUNT(" + sa.field + ")", "" + rows.size());
			}
			if (sa.type.equals("SUM")) {

				Double sum = 0D;
				for (int i = 0; i < rows.size(); i++) {
					Row r = rows.get(i);
					Double v;
					try {
						v = Double.valueOf(r.fields.get(sa.field));
					} catch (java.lang.NumberFormatException e) {
						throw new QueryException(
								"Trying aggregation on field with non-numeric field: "
										+ sa.type + "[" + sa.field + "] "
										+ r.fields.get(sa.field));
					}
					sum = sum + v;
				}

				Field f = new Field();
				f.name = "SUM";
				f.type = "AGG_SUM_" + sa.field;
				agg.fields.put(f, "" + sum);
			}
			if (sa.type.equals("AVG")) {
				Double sum = 0D;
				for (int i = 0; i < rows.size(); i++) {
					Row r = rows.get(i);
					Double v;
					try {
						v = Double.valueOf(r.fields.get(sa.field));
					} catch (java.lang.NumberFormatException e) {
						throw new QueryException(
								"Trying aggregation on field with non-numeric field: "
										+ sa.type + "[" + sa.field + "] "
										+ r.fields.get(sa.field));
					}
					sum = sum + v;
				}
				Double avg = sum / rows.size();

				Field f = new Field();
				f.name = "AVG";
				f.type = "AGG_AVG_" + sa.field;
				agg.fields.put(f, "" + avg);
			}
			if (sa.type.equals("MIN")) {
				Double min = null;
				for (int i = 0; i < rows.size(); i++) {
					Row r = rows.get(i);
					Double v;
					try {
						v = Double.valueOf(r.fields.get(sa.field));
					} catch (java.lang.NumberFormatException e) {
						throw new QueryException(
								"Trying aggregation on field with non-numeric field: "
										+ sa.type + "[" + sa.field + "] "
										+ r.fields.get(sa.field));
					}
					if (min == null) {
						min = v.doubleValue();
					}
					min = Math.min(min, v);
				}

				Field f = new Field();
				f.name = "MIN";
				f.type = "AGG_MIN_" + sa.field;
				agg.fields.put(f, "" + min);
			}

			if (sa.type.equals("MAX")) {
				Double max = null;
				for (int i = 0; i < rows.size(); i++) {
					Row r = rows.get(i);
					Double v;
					try {
						v = Double.valueOf(r.fields.get(sa.field));
					} catch (java.lang.NumberFormatException e) {
						throw new QueryException(
								"Trying aggregation on field with non-numeric field: "
										+ sa.type + "[" + sa.field + "] "
										+ r.fields.get(sa.field));
					}
					if (max == null) {
						max = v.doubleValue();
					}
					max = Math.max(max, v);
				}

				Field f = new Field();
				f.name = "MAX";
				f.type = "AGG_MAX_" + sa.field;
				agg.fields.put(f, "" + max);
			}

			// TODO - MAX,MIN,AVG,SUM - need numeric type ??

		}

		return agg;
	}

	public ArrayList<Row> groupShrink(ArrayList<Row> group) {
		ArrayList<Row> rows = new ArrayList<Row>();

		// FIXME - only the first or some other settings
		rows.add(group.get(0));

		return rows;
	}

	public ArrayList<Row> aggregationShrink(ArrayList<Row> rows0, Row agg) {
		ArrayList<Row> rows = new ArrayList<Row>();

		for (Row r0 : rows0) {
			Row r = new Row();
			r.fields.putAll(r0.fields);
			r.fields.putAll(agg.fields);
			rows.add(r);
		}

		rows.removeAll(rows.subList(1, rows.size()));

		return rows;
	}

	public ArrayList<Row> aggregationJoin(ArrayList<Row> rows0, Row agg) {
		ArrayList<Row> rows = new ArrayList<Row>();

		for (Row r0 : rows0) {
			Row r = new Row();
			r.fields.putAll(r0.fields);
			r.fields.putAll(agg.fields);
			rows.add(r);
		}

		return rows;
	}

	public ArrayList<Row> join() {
		ArrayList<Row> rows = new ArrayList<Row>();

		// 1. Prepare 2 list of rows
		ArrayList<ArrayList<Row>> rowsList = new ArrayList<ArrayList<Row>>();
		for (int i = 0; i < sq.tables.size(); i++) {
			ArrayList<Row> rowsi = new ArrayList<Row>();
			rowsi.addAll(sq.tables.get(i).rows);
			rowsList.add(rowsi);
		}

		// ArrayList<Row> rows0 = new ArrayList<Row>();
		// ArrayList<Row> rows1 = new ArrayList<Row>();
		// rows0.addAll(sq.tables.get(0).rows);
		// rows1.addAll(sq.tables.get(1).rows);

		// 2. Iterate each on each to match joining condition
		rows = rowsList.get(0);

		for (int i = 1; i < rowsList.size(); i++) {
			ArrayList<Row> rowsNext = new ArrayList<Row>();
			for (Row r0 : rows) {
				ArrayList<Row> rows1 = rowsList.get(i);
				for (Row r1 : rows1) {
					Row r = new Row();
					r.fields.putAll(r0.fields);
					r.fields.putAll(r1.fields);
					rowsNext.add(r);
				}
			}
			rows = new ArrayList<Row>();
			rows.addAll(rowsNext);
		}

		// for (Row r0:rows0){
		// for(Row r1:rows1){
		// Row r = new Row();
		// r.fields.putAll(r0.fields);
		// r.fields.putAll(r1.fields);
		// rows.add(r);
		// }
		// }

		return rows;
	}

	/**
	 * Filter table Rows and Columns
	 * 
	 * @return
	 */
	public ArrayList<Row> filter(ArrayList<Row> rows) {

		Iterator<Row> itRow = rows.iterator();
		while (itRow.hasNext()) {
			Row r = itRow.next();

			if (this.filterRowSimpleConditions(r)) {
				itRow.remove();
			}

			r = this.filterFields(r);
		}

		return rows;
	}

	public boolean filterRowHavingConditions(Row r) {
		boolean toRemoveTotal = false;
		boolean toKeepTotal = true;
		for (SimpleCondition sc : sq.havingSimpleConditions) {

			String v1;
			if (sc.field1Type.equals("C")) {
				v1 = r.fields.get(sc.field1);
			} else {
				v1 = sc.field1.name;
			}

			String v2;
			if (sc.field2Type.equals("C")) {
				v2 = r.fields.get(sc.field2);
			} else {
				v2 = sc.field2.name;
			}
			boolean toRemove = false;

			if (sc.type.equals("=")) {
				if (v1.compareTo(v2) != 0) {
					toRemove = true;
				}
			} else if (sc.type.equals("!=")) {
				if (v1.compareTo(v2) == 0) {
					toRemove = true;
				}
			} else if (sc.type.equals(">")) {
				if (v1.compareTo(v2) <= 0) {
					toRemove = true;
				}
			} else if (sc.type.equals("<")) {
				if (v1.compareTo(v2) >= 0) {
					toRemove = true;
				}
			} else if (sc.type.equals("<=")) {
				if (v1.compareTo(v2) > 0) {
					toRemove = true;
				}
			} else if (sc.type.equals(">=")) {
				if (v1.compareTo(v2) < 0) {
					toRemove = true;
				}
			}

			if (sc.logic.equals("AND")) {
				toRemoveTotal = toRemoveTotal || toRemove;
			} else if (sc.logic.equals("OR")) {
				toRemoveTotal = toRemoveTotal && toRemove;
			}
		}
		return toRemoveTotal;
	}

	/**
	 * Check conditions to remove Row
	 * 
	 * @param r
	 * @return
	 */
	public boolean filterRowSimpleConditions(Row r) {
		// Filter rows
		boolean toRemoveTotal = false;
		boolean toKeepTotal = true;
		for (SimpleCondition sc : sq.simpleConditions) {

			String v1;
			if (sc.field1Type.equals("C")) {
				v1 = r.fields.get(sc.field1);
			} else {
				v1 = sc.field1.name;
			}

			String v2;
			if (sc.field2Type.equals("C")) {
				v2 = r.fields.get(sc.field2);
			} else {
				v2 = sc.field2.name;
			}

			boolean toRemove = false;

			if (sc.type.equals("=")) {
				if (v1.compareTo(v2) != 0) {
					toRemove = true;
				}
			} else if (sc.type.equals("!=")) {
				if (v1.compareTo(v2) == 0) {
					toRemove = true;
				}
			} else if (sc.type.equals(">")) {
				if (v1.compareTo(v2) <= 0) {
					toRemove = true;
				}
			} else if (sc.type.equals("<")) {
				if (v1.compareTo(v2) >= 0) {
					toRemove = true;
				}
			} else if (sc.type.equals("<=")) {
				if (v1.compareTo(v2) > 0) {
					toRemove = true;
				}
			} else if (sc.type.equals(">=")) {
				if (v1.compareTo(v2) < 0) {
					toRemove = true;
				}
			} else if (sc.type.equals("IN")) {

				if ("IN".equals(sc.type)) {
					String[] values2 = v2.replaceAll("([\\[\\]]*)", "").split(
							",");
					boolean isIn = false;
					for (String val2 : values2) {
						if (v1.compareTo(val2) == 0) {
							isIn = true;
						}
					}
					toRemove = !isIn;
				}
			} else if (sc.type.equals("NOT IN")) {

				if ("NOT IN".equals(sc.type)) {
					String[] values2 = v2.replaceAll("([\\[\\]]*)", "").split(
							",");
					boolean isNotIn = true;
					for (String val2 : values2) {
						if (v1.compareTo(val2) == 0) {
							isNotIn = false;
						}
					}
					toRemove = !isNotIn;
				}
			}

			if (sc.logic.equals("AND")) {
				toRemoveTotal = toRemoveTotal || toRemove;
			} else if (sc.logic.equals("OR")) {
				toRemoveTotal = toRemoveTotal && toRemove;
			}
		}
		return toRemoveTotal;

	}

	/**
	 * Remove fields in a row
	 * 
	 * @param r
	 * @return
	 */
	public Row filterFields(Row r) {
		// Filter fields
		Iterator<Field> itField = r.fields.keySet().iterator();
		while (itField.hasNext()) {
			Field f = itField.next();
			if (!sq.fields.contains(f)
					&& !Arrays.asList(SimpleAggregation.types).contains(f)) {
				itField.remove();
			}
		}

		return r;
	}

	/**
	 * Print Result
	 * 
	 * @param rows
	 */
	public void printRowResult(ArrayList<Row> rows) {
		System.out.println("==============================================");
		if (rows.size() == 0) {
			System.out.println("No Result");
		}
		for (int i = 0; i < rows.size(); i++) {
			Row r = rows.get(i);
			if (i == 0) {
				for (Entry<Field, String> e : r.fields.entrySet()) {
					System.out.print(e.getKey() + ",");
				}

				System.out.println();
			}
			for (Entry<Field, String> e : r.fields.entrySet()) {
				System.out.print(e.getValue() + ",");
			}
			System.out.println();
		}
		System.out.println("==============================================");
	}

	public void printRowResult2(ArrayList<Row> rows, boolean headerLine) {
		System.out.println("==============================================");
		if (rows.size() == 0) {
			System.out.println("No Result");
		}

		Iterator<Row> it = rows.iterator();

		int c = 0;
		while (it.hasNext()) {
			Row r = it.next();
			if (c == 0) {
				Iterator<Entry<Field, String>> itFields = r.fields.entrySet()
						.iterator();
				int headLenght = 0;
				while (itFields.hasNext()) {
					Entry<Field, String> e = itFields.next();
					System.out.print(e.getKey());
					headLenght += e.getKey().name.length() + 1;
					if (itFields.hasNext()) {
						System.out.print(",");
					} else {
						System.out.println();
					}
				}
				if (headerLine) {
					for (int i = 1; i < headLenght; i++) {
						System.out.print("-");
					}
					System.out.println();
				}
			}

			Iterator<Entry<Field, String>> itFields = r.fields.entrySet()
					.iterator();
			while (itFields.hasNext()) {
				Entry<Field, String> e = itFields.next();
				System.out.print(e.getValue());
				if (itFields.hasNext()) {
					System.out.print(",");
				} else {
					System.out.println();
				}
			}

			c++;
		}
		System.out.println("==============================================");
	}

	public void printWithOrder(ArrayList<Row> rows, ArrayList<Field> fields,
			boolean headerLine) {
		System.out.println("==============================================");
		if (rows.size() == 0) {
			System.out.println("No Result");
		}

		Collections.sort(fields);

		int lineSize = 0;
		Iterator<Field> itField = fields.iterator();
		while (itField.hasNext()) {
			Field f = itField.next();

			System.out.print(f.name);
			if (f.aggField != null) {
				System.out.print("[" + f.aggField + "]");
				lineSize += f.aggField.length() + 2;
			}
			lineSize += f.name.length();

			if (itField.hasNext()) {
				System.out.print(",");
				lineSize++;
			} else {
				System.out.println();
			}

		}
		if (headerLine) {
			for (int k = 0; k < lineSize; k++) {
				System.out.print("=");
			}
			System.out.println();
		}

		Iterator<Row> it = rows.iterator();

		int c = 0;
		while (it.hasNext()) {
			Row r = it.next();

			for (int i = 0; i < fields.size(); i++) {
				Field fKey = new Field();
				fKey.name = fields.get(i).name;
				String v = r.fields.get(fKey);
				System.out.print(v);
				if (i + 1 < fields.size()) {
					System.out.print(",");
				} else {
					System.out.println();
				}

			}

		}

		System.out.println("==============================================");
	}
}
