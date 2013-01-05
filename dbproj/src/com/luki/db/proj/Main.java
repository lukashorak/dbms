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
		m.initData();
		m.start3();
		m.scanStdin();

	}

	public void initData() {
		this.db = new Database("data2");
		this.search = new Search();
	}

	public void start3() {
		this.db = new Database("data2");
		this.search = new Search();

		// System.out.println("----------------------------------------");
		// try {
		// this.initData();
		// search.search(db, "select * from category;");
		// } catch (QueryException e) {
		// e.printStackTrace();
		// }
		System.out.println("----------------------------------------");
		try {
			this.initData();
			search.search(db, "select id_category from category;");
		} catch (QueryException e) {
			e.printStackTrace();
		}
		System.out.println("----------------------------------------");
		// try {
		// this.initData();
		// search.search(
		// db,
		// "select min(ID_CATEGORY),max(ID_CATEGORY),sum(ID_CATEGORY),avg(ID_CATEGORY),count(ID_CATEGORY) from category;");
		// } catch (QueryException e) {
		// e.printStackTrace();
		// }
		System.out.println("----------------------------------------");
		try {
			this.initData();
			search.search(db, "select * from category where id_category=2;");
		} catch (QueryException e) {
			e.printStackTrace();
		}
		System.out.println("----------------------------------------");
		// try {
		// search.search(db, "select * from category where id_category>2;");
		// } catch (QueryException e) {
		// e.printStackTrace();
		// }
		// System.out.println("----------------------------------------");
		// try {
		// search.search(db, "select * from category where id_category>=2;");
		// } catch (QueryException e) {
		// e.printStackTrace();
		// }
		// System.out.println("----------------------------------------");
		// try {
		// search.search(db, "select * from category where id_category!=2;");
		// } catch (QueryException e) {
		// e.printStackTrace();
		// }
		// System.out.println("----------------------------------------");
		// try {
		// search.search(db,
		// "select * from category where id_category!=2 and id_category!=1;");
		// } catch (QueryException e) {
		// e.printStackTrace();
		// }
		// System.out.println("----------------------------------------");
		// try {
		// search.search(db,
		// "select * from category where id_category=1 or id_category=2;");
		// } catch (QueryException e) {
		// e.printStackTrace();
		// }
		// System.out.println("----------------------------------------");
		// try {
		// search.search(db,
		// "select * from category where id_category not in [1,2];");
		// } catch (QueryException e) {
		// e.printStackTrace();
		// }
		// System.out.println("----------------------------------------");
		// try {
		// search.search(db, "select * from category, product");
		// } catch (QueryException e) {
		// e.printStackTrace();
		// }
		// System.out.println("----------------------------------------");
		// try {
		// search.search(db,
		// "select * from category, product where id_category=category_id");
		// } catch (QueryException e) {
		// e.printStackTrace();
		// }
		// System.out.println("----------------------------------------");
		// try {
		// search.search(
		// db,
		// "select id_product,category_id,price,min(price),max(price) from product GROUP BY category_id");
		// } catch (QueryException e) {
		// e.printStackTrace();
		// }
		// System.out.println("----------------------------------------");
		// try {
		// search.search(
		// db,
		// "select id_product,category_id,price,min(price),max(price) from product GROUP BY category_id HAVING MAX[price] > 0 AND MAX[price] < 1600");
		// } catch (QueryException e) {
		// e.printStackTrace();
		// }
		System.out.println("----------------------------------------");
		try {
			search.search(
					db,
					"select id_product,category_id,price from product where category_id in (select id_category from category where id_category >1)");
		} catch (QueryException e) {
			e.printStackTrace();
		}
		System.out.println("----------------------------------------");
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
		// System.out.println("------------");
		// try {
		// search.search(db,
		// "select * from tab1,tab3,tab4 where col1 = colX ;");
		// } catch (QueryException e) {
		// e.printStackTrace();
		// }
		// System.out.println("------------");
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
		// System.out.println("----------------------------------------");
		// try {
		// search.search(
		// db,
		// "select col1,col2,count(*), min(col2),max(col2) from tab5 where col1 = 'D' group by col1 having col2 >= 3");
		// } catch (QueryException e) {
		// e.printStackTrace();
		// }
		// System.out.println("----------------------------------------");
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
		// System.out.println("----------------------------------------");
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
					this.initData();
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
