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
    private MyKeyPair keyPair = new MyKeyPair();
    private MyValuePair valuePair = new MyValuePair();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), ",");
        String message = new String();
        int nameValue = -1, index1 = -1, index2 = -1, val;
        while (itr.hasMoreTokens()) {
            String name;
            String M = new String("M");

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
                if(nameValue == 1){
                    keyPair.set(index1, k);
                    valuePair.set(nameValue, index2, val);
                    message += "( " + String.valueOf(keyPair.getI()) + "," + String.valueOf(keyPair.getK()) + " ) ";
                    message += "( " + String.valueOf(valuePair.getName()) + "," + String.valueOf(valuePair.getIndex())
                            + "," +  String.valueOf(valuePair.getValue()) + " )\n";
                    context.write(keyPair, valuePair);
                }else{
                    keyPair.set(k, index2);
                    valuePair.set(nameValue, index1, val);
                    context.write(keyPair, valuePair);
                    message += "( " + String.valueOf(keyPair.getI()) + "," + String.valueOf(keyPair.getK()) + " ) ";
                    message += "( " + String.valueOf(valuePair.getName()) + "," + String.valueOf(valuePair.getIndex())
                            + "," +  String.valueOf(valuePair.getValue()) + " )\n";
                }

            }
        
        }
        /*if(index1 > 0 && index2 > 0) {
            throw new IOException(message);
        }*/
    }
}




public static class IntSumReducer
        extends Reducer<MyKeyPair,MyValuePair,MyKeyPair,Text> {
    private IntWritable result = new IntWritable();
    private  Text t = new Text();

    public void reduce(MyKeyPair key, Iterable<MyValuePair> values,
                        Context context
                        ) throws IOException, InterruptedException{
        int sum = 0;
        int count = 0;
        int flag = 1;
        String message = new String();
        message += ("key: " + String.valueOf(key.getI()) + ", " + String.valueOf(key.getK()) + "\n" );
        for(MyValuePair val : values){
            count ++;
        }
        for (MyValuePair val1 : values) {
			for(MyValuePair val2 : values){
				if(val1.getName() != val2.getName() && val1.getIndex() == val2.getIndex()){
				    flag = -1;
					sum += val1.getValue() * val2.getValue();
				}
			}
			message += (String.valueOf(val1.getName()) + "," + String.valueOf(val1.getIndex()) + "," +  String.valueOf(val1.getValue()) + "\n");
        }
        /*if(sum == 0) {
            throw  new IOException(message);
        }*/
        result.set(sum);
        t.set(String.valueOf(key.getI()) + "," + String.valueOf(key.getK()) + "," + Integer.toString(sum) + ", flag = "  +  String.valueOf(flag)
        +  ", count = " + String.valueOf(count) + ", message = " + message);
        //t.set(String.valueOf(key.getI()) + "," + String.valueOf(key.getK()) + "," + Integer.toString(sum));
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

    /*job.setOutputKeyClass(MyKeyPair.class);
    job.setOutputValueClass(MyValuePair.class);*/

    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
