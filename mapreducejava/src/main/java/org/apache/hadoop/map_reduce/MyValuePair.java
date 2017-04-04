package org.apache.hadoop.map_reduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public  class MyValuePair implements Writable{
	private int name;
	private int index;
	private int value;
	public MyValuePair(int name, int index, int value){
		this.name = name;
		this.index = index;
		this.value = value;
	}
	public  MyValuePair(){
		this.name = 0;
		this.index = 0;
		this.value = 0;
	}
	public  void set(int name, int index, int value){
		this.name = name;
		this.index = index;
		this.value = value;
	}
	public int getName(){
		return name;
	}
	public int getIndex(){
		return index;
	}
	public int getValue(){
		return value;
	}
	public void write(DataOutput out) throws IOException {
		out.writeInt(name);
		out.writeInt(index);
		out.writeInt(value);
   }
   
   public void readFields(DataInput in) throws IOException {
	 this.name = in.readInt();
	 this.index = in.readInt();
	 this.value = in.readInt();
   }
   
   public static MyValuePair read(DataInput in) throws IOException {
	   MyValuePair w = new MyValuePair();
	 	w.readFields(in);
	 	return w;
   }

	@Override
	public String toString() {
		return "MyValuePair{" +
				"name=" + name +
				", index=" + index +
				", value=" + value +
				'}';
	}
}