package com.luki.db.proj.model;

public class Field implements Comparable<Field> {

	public Field() {

	}
	
	public Field(String name){
		this.name = name;
	}

	public String name;
	public String type;
	public Integer pos;
	public String aggField;

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Field other = (Field) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		}
		if (name != null && other.name!= null){
			return name.toLowerCase().equals(other.name.toLowerCase());
		}
		return true;
	}

	public int compareTo(Field arg0) {
		return this.pos.compareTo(arg0.pos);
	}

	@Override
	public String toString() {
		return name + "[type=" + type + ", pos=" + pos + "]";
	}

}
