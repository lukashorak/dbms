package com.luki.db.proj.model;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;

import au.com.bytecode.opencsv.CSVReader;

public class Database {

	public ArrayList<Table> tables = new ArrayList<Table>();
	String storageLocation = "data";

	public Database() {
		try {
			this.loadTablesFromFiles();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void loadTablesFromFiles() {

		System.out.println("Read files");

		File f = new File(storageLocation);
		String[] list = f.list();

		for (String s : list) {
			System.out.println("\t" + s);
			try {
				Table t = this.loadTable(s);
				this.tables.add(t);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private Table loadTable(String tableCsvFile) throws Exception {
		CSVReader reader = new CSVReader(new FileReader(storageLocation + "/"
				+ tableCsvFile));
		String[] nextLine;

		Table t = new Table();
		t.name = tableCsvFile.toLowerCase().replaceAll(".csv", "");
		// Read Header
		nextLine = reader.readNext();

		if (nextLine != null) {
			for (int i = 0; i < nextLine.length; i++) {
				String s = nextLine[i];
				t.header.add(s);
			}
		}
		// Read Content

		while ((nextLine = reader.readNext()) != null) {
			Row r = new Row();

			for (int i = 0; i < nextLine.length; i++) {
				Field f = new Field();
				f.name = t.header.get(i);
				f.pos = i;
				r.fields.put(f, nextLine[i]);
			}
			t.rows.add(r);
		}

		return t;
	}

}
