package org.apache.hadoop.map_reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;



public class MyKeyPair implements WritableComparable<MyKeyPair> {
	private IntWritable i;
	private IntWritable k;
	public MyKeyPair(){
		this.i = new IntWritable(0);
		this.k = new IntWritable(0);
	}
	public MyKeyPair(int i, int k){
		this.i = new IntWritable(i);
		this.k = new IntWritable(k);
		//this.i.set(i);
		//this.k.set(k);
	}
	public IntWritable getI(){
		return i;
	}
	public IntWritable getK(){
		return k;
	}
	public void write(DataOutput out) throws IOException {
	 i.write(out);
	 k.write(out);
   }
   
   public void readFields(DataInput in) throws IOException {
	 i.readFields(in);
	 k.readFields(in);
   }

    @Override
    public int compareTo(MyKeyPair o) {
	    return (o.i.get() > this.i.get() ? 0 : (o.i.get() == this.i.get() ? (o.k.get() > this.k.get() ? 0 : 1 ): 1));
    }
}