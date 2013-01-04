package com.luki.db.proj;

import java.util.ArrayList;
import java.util.Date;
import java.util.Scanner;

import com.luki.db.proj.model.Database;
import com.luki.db.proj.model.Table;
import com.luki.db.proj.query.QueryException;

public class Main {

	ArrayList<Table> tables;
	Database db;
	Search search;

	public static void main(String[] args) throws Exception {

		Main m = new Main();
		m.start2();

	}

	public void start2() {
		this.db = new Database();
		this.search = new Search();

		// try {
		// search.search(db,
		// "select col1,col2 from tab1 where col1 = a or col1 = b OR col1 = c;");
		// } catch (QueryException e) {
		// e.printStackTrace();
		// }
		// System.out.println("------------");
		// try {
		// search.search(db, "select * from tab3 where colA = col1;");
		// } catch (QueryException e) {
		// e.printStackTrace();
		// }
		// System.out.println("------------");
		// try {
		// search.search(db, "select * from ;");
		// } catch (QueryException e) {
		// e.printStackTrace();
		// }
		 System.out.println("------------");
		 try {
		 search.search(db,
		 "select * from tab1,tab3,tab4 where col1 = colX ;");
		 } catch (QueryException e) {
		 e.printStackTrace();
		 }
		 System.out.println("------------");
		// try {
		// search.search(db, "select count(*) from tab1, tab3;");
		// } catch (QueryException e) {
		// e.printStackTrace();
		// }
		// System.out.println("----------------------------------------");
		// try {
		// search.search(db,
		// "select col2,sum(col2),count(col2),avg(col2),min(col2),max(col2) from tab1;");
		// } catch (QueryException e) {
		// e.printStackTrace();
		// }
		// System.out.println("----------------------------------------");
		// try {
		// search.search(
		// db,
		// "select col1,col2,count(*), min(col2),max(col2) from tab5 where col1!='A' group by col1 ");
		// } catch (QueryException e) {
		// e.printStackTrace();
		// }
//		System.out.println("----------------------------------------");
//		try {
//			search.search(
//					db,
//					"select col1,col2,count(*), min(col2),max(col2) from tab5 where col1 = 'D' group by col1 having col2 >= 3");
//		} catch (QueryException e) {
//			e.printStackTrace();
//		}
//		System.out.println("----------------------------------------");
		// try {
		// search.search(
		// db,
		// "select col1,col2,count(*), min(col2),max(col2) from tab5 group by col1 having col2 >= 3");
		// } catch (QueryException e) {
		// e.printStackTrace();
		// }
		//
		// System.out.println("----------------------------------------");
		// try {
		// search.search(db,
		// "SELECT col1 FROM tab1");
		// } catch (QueryException e) {
		// e.printStackTrace();
		// }

		// System.out.println("----------------------------------------");
		// FIXME
		// try {
		// search.search(
		// db,
		// "select * from tab1 where col1  in (select col1 from tab1 where col1 in (select col1 from tab1));");
		// } catch (QueryException e) {
		// e.printStackTrace();
		// }
		System.out.println("----------------------------------------");
		// try {
		// search.search(
		// db,
		// "select * from tab1 where col1 in (select colA from tab2) and col2 in (select colB from tab3);");
		// } catch (QueryException e) {
		// e.printStackTrace();
		// }
		// System.out.println("----------------------------------------");
		// try {
		// search.search(db,
		// "select * from tab1 where col1 ='A' ;");
		// } catch (QueryException e) {
		// e.printStackTrace();
		// }
		// System.out.println("----------------------------------------");
		// try {
		// search.search(db, "select col1,col3,count(*),col2 from tab1;");
		// } catch (QueryException e) {
		// e.printStackTrace();
		// }
		// System.out.println("----------------------------------------");
	}

	public void start() {
		this.db = new Database();
		this.search = new Search();

		try {
			search.search(db, "select * from tab1;");
		} catch (QueryException e) {
			e.printStackTrace();
		}
		System.out.println("------------");
		try {
			search.search(db, "select col1,col2 from tab1;");
		} catch (QueryException e) {
			e.printStackTrace();
		}
		System.out.println("------------");
		try {
			search.search(db, "select * from tab1 where col1 = 5;");
		} catch (QueryException e) {
			e.printStackTrace();
		}
		System.out.println("------------");
		try {
			search.search(db,
					"select * from tab1 where col1 = 5 AND col2 >= 6 OR col3 != col2;");
		} catch (QueryException e) {
			e.printStackTrace();
		}
		System.out.println("------------");
		try {
			search.search(db, "select * from tab1,tab3 where col1 = 5;");
		} catch (QueryException e) {
			e.printStackTrace();
		}
		System.out.println("------------");
		try {
			search.search(db, "select count(*) from tab1");
		} catch (QueryException e) {
			e.printStackTrace();
		}
		System.out.println("------------");
		try {
			search.search(db, "select count(col1),col2 from tab1");
		} catch (QueryException e) {
			e.printStackTrace();
		}
		System.out.println("------------");
		try {
			search.search(db, "select min(col1) from tab1");
		} catch (QueryException e) {
			e.printStackTrace();
		}
		System.out.println("------------");
		try {
			search.search(db, "select min(col1) from tab1 group by col1");
		} catch (QueryException e) {
			e.printStackTrace();
		}
		System.out.println("------------");
		try {
			search.search(db,
					"select min(col1) from tab1 group by col1 having count(*) > 2");
		} catch (QueryException e) {
			e.printStackTrace();
		}
		// this.scanStdin();
		System.out.println("==========================================");
	}

	public void scanStdin() {
		String sQuery;

		Scanner scanIn = new Scanner(System.in);
		do {
			System.out.println("Enter query (\"exit\" for quit) :");
			sQuery = scanIn.nextLine();

			if (!sQuery.equals("exit")) {
				System.out.println("Searching :" + sQuery);
				Long startTime = new Date().getTime();

				try {
					search.search(this.db, sQuery);
				} catch (QueryException e) {
					e.printStackTrace();
				}

				Long endTime = new Date().getTime();
				System.out.println("Time to execture query :"
						+ (endTime - startTime) + " milis");
			}
		} while (!sQuery.equals("exit") && scanIn.hasNext());
		scanIn.close();
	}

}
