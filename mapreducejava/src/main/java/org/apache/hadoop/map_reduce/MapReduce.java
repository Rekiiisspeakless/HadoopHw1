package org.apache.hadoop.map_reduce;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MapReduce {

    public static class TokenizerMapper
        extends Mapper<Object, Text, MyKeyPair, MyValuePair>{
    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), ",");
        while (itr.hasMoreTokens()) {
		String name;
		String M = new String("M");
		int nameValue, index1, index2, val;
		name = itr.nextToken();
		if(name.equals(M)){
			nameValue = 1;
		}else{
			nameValue = 2;
		}
		index1 = Integer.valueOf(itr.nextToken()).intValue();
		index2 = Integer.valueOf(itr.nextToken()).intValue();
		val = Integer.valueOf(itr.nextToken()).intValue();
		for(int k = 0; k < 3; k++){
			MyKeyPair keyPair = new MyKeyPair(index1, k);
			MyValuePair valuePair = new MyValuePair(nameValue, index2, val);
			context.write(keyPair, valuePair);
		}
        
        }
    }
}




public static class IntSumReducer
        extends Reducer<MyKeyPair,MyValuePair,MyKeyPair,Text> {
    private IntWritable result = new IntWritable();

    public void reduce(MyKeyPair key, Iterable<MyValuePair> values,
                        Context context
                        ) throws IOException, InterruptedException {
        int sum = 0;
        for (MyValuePair val1 : values) {
			for(MyValuePair val2 : values){
				if(val1.getName().get() != val2.getName().get() && val1.getIndex().get() == val2.getIndex().get()){
					sum += val1.getValue().get() * val2.getValue().get();
				}
			}
        }
       result.set(sum);
        Text t = new Text();
        t.set(key.getI().toString() + "," + key.getK().toString() + "," + Integer.toString(sum));
       context.write(null, t);
    }
}

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
        System.err.println("Usage: map_reduce <in> <out>");
        System.exit(2);
    }
    Job job = new Job(conf, "map reduce");
    job.setJarByClass(MapReduce.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setMapOutputKeyClass(MyKeyPair.class);
    job.setMapOutputValueClass(MyValuePair.class);
    job.setOutputKeyClass(MyKeyPair.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}