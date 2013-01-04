package com.luki.db.proj.query;

public class QueryNormalizer {
	
	public String normalize(String s){
		
		String newS = String.valueOf(s);
		newS = this.makeLowerCaseExceptQuotes(newS);
		newS = newS.replaceAll("(\\s+)", " ");
		newS = newS.replaceAll("(\\s+;)", ";");
		newS = newS.replaceFirst("(\\s*;?\\s*)$", ";");
		
		newS = newS.replaceAll("(\\s*,\\s*)", ",");
		//newS = newS.toLowerCase();
		
		newS = newS.replaceAll("(\\s*[(]\\s*)", " (");
		newS = newS.replaceAll("(\\s*[)]\\s*)", ") ");
		
		newS = newS.replace("select ", "SELECT ");
		newS = newS.replace("from ", "FROM ");
		newS = newS.replace("where ", "WHERE ");
		
		newS = newS.replace("and ", "AND ");
		newS = newS.replace("or ", "OR ");
		
		newS = newS.replace("count ", "COUNT ");
		newS = newS.replace("sum ", "SUM (");
		newS = newS.replace("max ", "MAX ");
		newS = newS.replace("min ", "MIN ");
		newS = newS.replace("avg ", "AVG ");
		
		newS = newS.replace("not in ", "NOT IN ");
		newS = newS.replace("in ", "IN ");
		newS = newS.replace("group by ", "GROUP BY ");
		newS = newS.replace("having ", "HAVING ");

		
		newS = newS.replace("show tables", "SHOW TABLES");
		newS = newS.replace("desc ", "DESC ");
		
		return newS;
	}
	
	public String makeLowerCaseExceptQuotes(String s){
		StringBuffer sb = new StringBuffer();

		Character quote = new Character('\'');
		boolean inQuotes = false;		 
		for (Character c : s.toCharArray()){
			if (quote.equals(c)){
				inQuotes = !inQuotes;
			}
			if (!inQuotes){
				sb.append(c.toLowerCase(c));
			}else{
				sb.append(c);
			}
			
		}
		return sb.toString();
	}

}
