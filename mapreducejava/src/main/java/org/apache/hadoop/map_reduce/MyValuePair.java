package org.apache.hadoop.map_reduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public  class MyValuePair implements Writable{
	private IntWritable name;
	private IntWritable index;
	private IntWritable value;
	public MyValuePair(int name, int index, int value){
		this.name = new IntWritable(name);
		this.index = new IntWritable(index);
		this.value = new IntWritable(value);
	}
	public  MyValuePair(){
		this.name = new IntWritable(0);
		this.index = new IntWritable(0);
		this.value = new IntWritable(0);
	}
	public IntWritable getName(){
		return name;
	}
	public IntWritable getIndex(){
		return index;
	}
	public IntWritable getValue(){
		return value;
	}
	public void write(DataOutput out) throws IOException {
	 name.write(out);
	 index.write(out);
	 value.write(out);
   }
   
   public void readFields(DataInput in) throws IOException {
	 name.readFields(in);
	 index.readFields(in);
	 value.readFields(in);
   }
   
   public static MyValuePair read(DataInput in) throws IOException {
	   MyValuePair w = new MyValuePair(in.readInt(), in.readInt(), in.readInt());
	 	w.readFields(in);
	 return w;
   }
}