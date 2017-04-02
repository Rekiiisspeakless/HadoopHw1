package org.apache.hadoop.map_reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;



public class MyKeyPair implements WritableComparable<MyKeyPair> {
	private int i;
	private int k;
	public MyKeyPair(){
		this.i = 0;
		this.k = 0;
	}
	public MyKeyPair(int i, int k){
		this.i = i;
		this.k = k;
		//this.i.set(i);
		//this.k.set(k);
	}
	public int getI(){
		return i;
	}
	public int getK(){
		return k;
	}
	public void write(DataOutput out) throws IOException {
	 out.writeInt(i);
	 out.writeInt(k);
   }
   
   public void readFields(DataInput in) throws IOException {
	 i = in.readInt();
	 k = in.readInt();
   }


   public static  MyKeyPair read(DataInput in) throws IOException{
		MyKeyPair  myKeyPair = new MyKeyPair();
		myKeyPair.readFields(in);
		return  myKeyPair;
   }

    @Override
    public int compareTo(MyKeyPair o) {
	    int value1 = this.i - o.getI();
	    int value2 = this.k - o.getK();
	    if(value1 != 0){
	    	return  value1;
		}else{
	    	return value2;
		}
    }
}