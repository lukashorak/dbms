package com.luki.db.proj;

import com.luki.db.proj.model.Database;
import com.luki.db.proj.model.Field;
import com.luki.db.proj.model.Table;
import com.luki.db.proj.query.QueryException;
import com.luki.db.proj.query.QueryExecutor;
import com.luki.db.proj.query.QueryNormalizer;
import com.luki.db.proj.query.SelectQuery;

public class Search {

	QueryNormalizer qn = new QueryNormalizer();
	Database db;

	public Search() {

	}

	public void search(Database db, String query) throws QueryException {
		this.db = db;

		System.out.println("Q =\t" + query);
		String normalizedQuery = qn.normalize(query);
		System.out.println("Qnorm =\t" + normalizedQuery);

		if ("SHOW TABLES;".equals(normalizedQuery)) {
			this.showTables();
			return;
		}
		if (normalizedQuery.startsWith("DESC ")) {
			String[] params = normalizedQuery.split("(\\s+)");
			this.descTable(params[1]);
			return;
		}
		if (normalizedQuery.startsWith("SELECT ")) {
			SelectQuery sq = new SelectQuery(this.db);
			sq.parse(normalizedQuery);

			QueryExecutor exec = new QueryExecutor();
			exec.executeWithShow(sq);

		}

	}

	public void showTables() {
		System.out.println("Tables:");

		for (int i = 0; i < db.tables.size(); i++) {
			Table t = db.tables.get(i);
			System.out.println(i + "\t" + t.name);
		}
		System.out.println("---------------");
	}

	public void descTable(String s) {

		for (Table t : db.tables) {
			if (t.name.equals(s)) {

				System.out.println(t.name);
				System.out.println("----------------");
				for (String h : t.header) {
					System.out.println(h);
				}
				System.out.println("----------------");
				System.out.println("Row count = " + t.rows.size());
				return;
			}
		}
	}

}
