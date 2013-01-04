package com.luki.db.proj.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.luki.db.proj.model.Database;
import com.luki.db.proj.model.Field;
import com.luki.db.proj.model.SimpleAggregation;
import com.luki.db.proj.model.SimpleCondition;
import com.luki.db.proj.model.Table;

public class SelectQuery {

	public Database db;

	public boolean inSubquery;
	public ArrayList<String> inSubqueryQueries = new ArrayList<String>();

	public ArrayList<Table> tables = new ArrayList<Table>();

	public boolean allFields = false;
	public ArrayList<Field> fields = new ArrayList<Field>();
	// public ArrayList<SimpleField> simpleFields = new
	// ArrayList<SimpleField>();

	public ArrayList<SimpleAggregation> simpleAggregations = new ArrayList<SimpleAggregation>();

	public ArrayList<SimpleCondition> simpleConditions = new ArrayList<SimpleCondition>();

	public boolean groupBy = false;
	public ArrayList<Field> groupByFields = new ArrayList<Field>();

	public boolean having = false;
	public ArrayList<String> havingConditions = new ArrayList<String>();
	public ArrayList<SimpleCondition> havingSimpleConditions = new ArrayList<SimpleCondition>();

	public SelectQuery(Database db) {
		this.db = db;
	}

	public class SimpleField {
		public String field;
		public Integer pos;
	}

	public void parse(String q) throws QueryException {
		q = this.parseInSubqueryRegex(q);
		this.parseTables(q);
		this.parseFields(q);
		this.parseConditions(q);

		this.parseGroupBy(q);
		this.parseHaving(q);
	}

	public String parseInSubqueryRegex(String q) throws QueryException {
		System.out.println("SUBQUERY ");
		String regexInSubquery = "IN \\((SELECT[\\s\\w\\=\\,\\'<>\\[\\]]*)\\)";

		Pattern pattern = Pattern.compile(regexInSubquery);
		Matcher matcher = pattern.matcher(q);

		ArrayList<String> inSubqueryResult = new ArrayList<String>();
		// Check all occurance
		int c = 0;
		while (matcher.find()) {

			this.inSubquery = true;
			System.out.print("SQ" + c + "\t");
			System.out.print("Start index: " + matcher.start());
			System.out.print(" End index: " + matcher.end() + " ");
			System.out.println(matcher.group(1));
			inSubqueryQueries.add(matcher.group(1));

			System.out.println("=======SUB QUERY START==========");
			SelectQuery sq2 = new SelectQuery(this.db);
			sq2.parse(this.inSubqueryQueries.get(c) + ";");
			QueryExecutor exec2 = new QueryExecutor();
			String result = exec2.executeSubQuery(sq2);
			System.out.println("=======SUB QUERY END==========");
			inSubqueryResult.add(result);
			System.out.println("SQ" + c + "\t" + result);
			q = matcher.replaceAll("IN " + result + "");
			c++;

			matcher = pattern.matcher(q);
		}

		for (int i = 0; i < this.inSubqueryQueries.size(); i++) {
			// TODO - Execute subquery and put result in format
			// [value1,value2,value3]

			// System.out.println("=======SUB QUERY START==========");
			// SelectQuery sq2 = new SelectQuery(this.db);
			// sq2.parse(this.inSubqueryQueries.get(i) + ";");
			// QueryExecutor exec2 = new QueryExecutor();
			// String result = exec2.executeSubQuery(sq2);
			// System.out.println("=======SUB QUERY END==========");

			// q = q.replace(this.inSubqueryQueries.get(i), result);
			// q = q.replace(this.inSubqueryQueries.get(i), "[XXX" + i + "]");
		}

		System.out.println("SQchanged=" + q);
		System.out.println("======================");
		return q;
	}

	@Deprecated
	// Don't work too good
	public String parseInSubquery(String q) throws QueryException {
		System.out.println("SUBQUERY ");

		int s1 = q.lastIndexOf("IN (SELECT ");
		if (s1 >= 0) {
			s1 = s1 + 4;
		}
		int s2 = q.lastIndexOf(")");

		if (s1 == -1) {
			this.inSubquery = false;
			System.out.println("NO SUBQUERY");
			return q;
		}
		this.inSubquery = true;
		String inSubquery = q.substring(s1, s2).trim();
		System.out.println("SQ=\t" + inSubquery);

		// TODO - Execute subquery and put result in format
		// [value1,value2,value3]
		q = q.replace("(" + inSubquery + ")", "[XXX1]");

		System.out.println("SQchanged=\t" + q);

		System.out.println("======================");
		return q;
	}

	@Deprecated
	// just stub
	public void parseSubQuery(String q) {
		System.out.println("SUB QUERY");
		int s1 = q.lastIndexOf("IN (");
		if (s1 >= 0) {
			s1 = s1 + 6;
		}
		int s2 = q.lastIndexOf(")");

		String subQueryRegex = "(IN\\s*\\(([\\w\\s\\d=\\бебж]*)\\))";
	}

	public void parseHaving(String q) throws QueryException {
		System.out.println("HAVING ");

		int s1 = q.lastIndexOf("HAVING ");
		if (s1 >= 0) {
			s1 = s1 + 7;
		}
		int s2 = q.lastIndexOf(";");

		if (s1 == -1 || s2 == -1) {
			this.having = false;
			System.out.println("NO HAVING");
			return;
		}

		String having = q.substring(s1, s2).trim();
		System.out.println("H=\t" + having);

		this.havingSimpleConditions.addAll(ConditionsFinder
				.findSimpleCondition(having, this.fields));

		// TODO - HavingAggregation

		System.out.println("Hparse:");

		for (SimpleCondition sc : this.havingSimpleConditions) {
			System.out.println("\t" + sc.logic + "\t" + sc.field1 + "("
					+ sc.field1Type + ")" + "\t" + sc.type + "\t" + sc.field2
					+ "(" + sc.field2Type + ")");
		}

		if (!this.groupBy && this.having) {
			throw new QueryException("Can't use HAVING without GROUP BY");
		}

		System.out.println("======================");
	}

	public void parseGroupBy(String q) throws QueryException {
		System.out.println("GROUP BY ");

		int s1 = q.lastIndexOf("GROUP BY ");
		if (s1 >= 0) {
			s1 = s1 + 9;
		}
		int s2 = q.lastIndexOf("HAVING ");
		if (s2 == -1) {
			s2 = q.lastIndexOf(";");
		}

		if (s1 == -1 || s2 == -1) {
			this.groupBy = false;
			System.out.println("NO GROUP BY");
			return;
		} else {
			this.groupBy = true;
		}

		String groupBy = q.substring(s1, s2).trim();
		System.out.println("G=\t" + groupBy);

		ArrayList<String> gList = new ArrayList<String>();
		gList.addAll(Arrays.asList(groupBy.split("(,)")));

		for (Table t : this.tables) {
			for (String h : t.header) {
				if (gList.contains(h)) {
					Field f = new Field();
					f.name = h;
					this.groupByFields.add(f);
					gList.remove(h);
				}
			}
		}

		if (gList.size() > 0) {
			throw new QueryException("Field " + gList.toString()
					+ " doesn't exist");
		}

		System.out.print("Gparse=\t");
		for (Field f : this.groupByFields) {
			System.out.print(f + ",");
		}
		System.out.println();

		System.out.println("======================");
	}

	// FIXME - SUBQUERY
	public void parseConditions(String q) {
		System.out.println("CONDITIONS");

		int s1 = q.lastIndexOf("WHERE ");
		if (s1 >= 0) {
			s1 = s1 + 6;
		} else {
			// No Conditions
			System.out.println("NO CONDITIONS");
			return;
		}
		int s2 = q.lastIndexOf("GROUP BY ");
		if (s2 == -1) {
			s2 = q.lastIndexOf(";");
		}
		

		String conditions = q.substring(s1, s2).trim();
		System.out.println("C=\t" + conditions);

		// Find Conditions
		this.simpleConditions.addAll(ConditionsFinder.findSimpleCondition(
				conditions, this.fields));

		System.out.println("Cparse:\t");

		for (SimpleCondition sc : this.simpleConditions) {
			System.out.println("\t" + sc.logic + "\t" + sc.field1 + "("
					+ sc.field1Type + ")" + "\t" + sc.type + "\t" + sc.field2
					+ "(" + sc.field2Type + ")");
		}

		// ""

		System.out.println("======================");
	}

	public void parseTables(String q) throws QueryException {
		System.out.println("TABLES");

		int s1 = q.lastIndexOf("FROM ");
		if (s1 >= 0) {
			s1 = s1 + 5;
		}
		int s2 = q.lastIndexOf("WHERE ");
		if (s2 == -1) {
			s2 = q.lastIndexOf("GROUP BY ");
		}
		if (s2 == -1) {
			s2 = q.lastIndexOf(";");
		}
		if (s1 == -1 || s2 == -1) {
			throw new QueryException("No Table in SELECT");
		}

		String tables = q.substring(s1, s2).trim();
		System.out.println("T=\t" + tables);

		ArrayList<String> tList = new ArrayList<String>();
		tList.addAll(Arrays.asList(tables.split("(,)")));

		for (Table t : this.db.tables) {
			if (tList.contains(t.name)) {
				this.tables.add(t);
				tList.remove(t.name);
			}
		}

		if (tList.size() > 0) {
			throw new QueryException("Table " + tList.toString()
					+ " doesn't exist in DB");
		}

		System.out.print("Tparse=\t");
		for (Table t : this.tables) {
			System.out.print(t.name + ",");
		}
		System.out.println();

		System.out.println("======================");
	}

	public void parseFields(String q) throws QueryException {
		System.out.println("FIELDS");

		int s1 = q.lastIndexOf("SELECT ");
		if (s1 >= 0) {
			s1 = s1 + 7;
		}
		int s2 = q.lastIndexOf("FROM ");

		String fields = q.substring(s1, s2).trim();
		System.out.println("F=\t" + fields);

		if ("*".equals(fields)) {
			this.allFields = true;

			int c = 0;
			for (Table t : this.tables) {
				for (String h : t.header) {
					Field f = new Field();
					f.name = h;
					f.pos = c;

					this.fields.add(f);
					c++;
				}
			}
		} else {
			ArrayList<String> fListString = new ArrayList<String>();
			ArrayList<String> fListPosition = new ArrayList<String>();
			fListString.addAll(Arrays.asList(fields.split("(,)")));
			fListPosition.addAll(Arrays.asList(fields.split("(,)")));

			// ArrayList<Field> fList = new ArrayList<Field>();
			// for (int i = 0;i<fListString.size();i++){
			//
			// String s = fListString.get(i);
			// Field f = new Field();
			// f.name = s;
			// f.pos = i;
			// fList.add(f);
			// }

			// FIXME - same name in different table
			int c = 0;
			for (Table t : this.tables) {
				for (String h : t.header) {
					if (fListString.contains(h)) {
						Field f = new Field();
						f.name = h;
						f.pos = fListPosition.indexOf(h);
						this.fields.add(f);
						fListString.remove(h);
						c++;
					}
				}
			}

			// TODO Aggregation
			if (fListString.size() > 0) {

				Iterator<String> it = fListString.iterator();
				while (it.hasNext()) {
					String f = it.next();
					SimpleAggregation sa = new SimpleAggregation();
					for (String type : SimpleAggregation.types) {
						if (f.contains(type)) {
							sa.type = type;
							String field = f.replace(type, "");
							field = field.replaceAll("\\(", "");
							field = field.replaceAll("\\)", "");
							field = field.trim();

//							sa.field = field;
							sa.field = new Field(field);
							if ("*".equals(field)) {
								sa.allField = true;
							}

							Field fieldObj = new Field();
							fieldObj.name = type;
							fieldObj.pos = fListPosition.indexOf(f);
							fieldObj.type = "AGG";
							fieldObj.aggField = field;
							this.fields.add(fieldObj);
							break;
						}
					}
					if (sa.type != null) {

						this.simpleAggregations.add(sa);
						it.remove();
					}
				}
			}

			if (fListString.size() > 0) {
				throw new QueryException("Field " + fListString.toString()
						+ " doesn't exist");
			}
		}

		System.out.print("Fparse=\t");
		Iterator<Field> itFields = this.fields.iterator();
		while (itFields.hasNext()) {
			Field f = itFields.next();
			System.out.print(f);
			if (itFields.hasNext()) {
				System.out.print(",");
			}
		}
		System.out.print("\t|\t");
		Iterator<SimpleAggregation> itAgg = this.simpleAggregations.iterator();
		while (itAgg.hasNext()) {
			SimpleAggregation sa = itAgg.next();
			System.out.print(sa.type + "[" + sa.field + "]");
			if (itAgg.hasNext()) {
				System.out.print(",");
			}
		}
		System.out.println();

		System.out.println("======================");
	}

}
