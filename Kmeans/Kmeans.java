package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapred.Counters;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*; 
import java.util.HashMap;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;
import java.math.*; 
import java.net.URI;

import org.apache.hadoop.fs.FSDataInputStream;  
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;

import org.apache.commons.math3.analysis.function.Add;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;

public class Kmeans {
	public static final int d = 58;
	public static final int centerNum = 10;
	/*
	public static class Center{
		//int point[3];
		public static String str;
		public static String[] element = new String[58];
		public Center(String s){
			this.str = s;
		};
		public String getStr(){
			return str;
		}
		public void setStr(String s){
			this.str = s;
		}
		public static String getElement(int i){
			return element[i];
		}
		public static void setElement(int i, String s){
			element[i] = s;
		}
	}
	
	public static class Cost{
		public static int round = 0;
		public static double[] cost = new double[50];
		public static void setCost( double ans){
			cost[round] = ans;
			round +=1;
		}
		public static double getCost(int i){
			return cost[i];
		}
	}
	*/
	
    public static class EuclideanMapper
        extends Mapper<Object, Text, Text, Text>{

		private Text word = new Text();		
		public Text testK = new Text();
		public double[][] center = new double[centerNum][d]; 
		
		///read center file
		public void setup(Context context) throws IOException, InterruptedException{
			URI[] files = context.getCacheFiles();
			URI uri = files[0];
			FileReader fr = new FileReader( new File(uri.getFragment()).toString() );
			BufferedReader bf = new BufferedReader(fr);
			int center_i = 0;
			String line;
			while(bf.ready()){
				line = bf.readLine().toString();
				String tokens[] = line.split("\\s+");
				if(tokens.length > 2){	/// not cost
					for(int i=0; i< tokens.length; i++){
						center[center_i][i] = Double.parseDouble(tokens[i]);
					}
					center_i += 1;
				}
			}
		}
		public void map(Object key, Text value, Context context
						) throws IOException, InterruptedException {
			String line = value.toString();
			String tokens[] = line.split("\\s+"); 
			int centroid = -1;
			double cost = 99999.0;
			for(int i=0; i<centerNum; i++){
				double sum = 0.0;
				for(int j=0; j<d; j++){
					sum += Math.pow( center[i][j]-Double.parseDouble(tokens[j]), 2 );
				}
				if(sum < cost || centroid == -1){
					cost = sum;
					centroid = i;
				}
			}
			Text k = new Text();
			Text v = new Text();
			k.set( String.valueOf(centroid) );
			context.write(k, value);
			
			context.write(new Text("cost"), new Text(String.valueOf(cost)));
				
		}
	}

	public static class EuclideanReducer
			extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values,
							Context context
							) throws IOException, InterruptedException {
			if(key.toString().equals("cost")){
				double cost = 0.0;
				for (Text val : values) {
					cost += Double.parseDouble(val.toString());
				}
				context.write(null, new Text( "cost " + String.valueOf(cost)));				
			}
			else{
				double newCentroid[] = new double[d];
				int pointNum = 0;
				for (Text val : values) {
					String line = val.toString();
					String tokens[] = line.split("\\s+"); 
					for(int i=0; i<d; i++){
						newCentroid[i] += Double.parseDouble(tokens[i]);
					}
					pointNum += 1;					
				}
				String centroidStr = "";
				for(int i=0; i<d; i++){
					newCentroid[i] = newCentroid[i]/(double)pointNum;
					centroidStr += String.valueOf(newCentroid[i]);
					centroidStr += " ";
				}
				context.write(null, new Text(centroidStr));
			}
		}
	}
//======================================================================
	 public static class ManhattanMapper
        extends Mapper<Object, Text, Text, Text>{

		private Text word = new Text();		
		public Text testK = new Text();
		public double[][] center = new double[centerNum][d]; 
		
		///read center file
		public void setup(Context context) throws IOException, InterruptedException{
			URI[] files = context.getCacheFiles();
			URI uri = files[0];
			FileReader fr = new FileReader( new File(uri.getFragment()).toString() );
			BufferedReader bf = new BufferedReader(fr);
			int center_i = 0;
			String line;
			while(bf.ready()){
				line = bf.readLine().toString();
				String tokens[] = line.split("\\s+");
				if(tokens.length > 2){	/// not cost
					for(int i=0; i< tokens.length; i++){
						center[center_i][i] = Double.parseDouble(tokens[i]);
					}
					center_i += 1;
				}
			}
		}
		public void map(Object key, Text value, Context context
						) throws IOException, InterruptedException {
			String line = value.toString();
			String tokens[] = line.split("\\s+"); 
			int centroid = -1;
			double cost = 99999.0;
			for(int i=0; i<centerNum; i++){
				double sum = 0.0;
				for(int j=0; j<d; j++){
					sum += Math.abs( center[i][j]-Double.parseDouble(tokens[j]) );
				}
				if(sum < cost || centroid == -1){
					cost = sum;
					centroid = i;
				}
			}
			Text k = new Text();
			Text v = new Text();
			k.set( String.valueOf(centroid) );
			context.write(k, value);
			
			context.write(new Text("cost"), new Text(String.valueOf(cost)));
				
		}
	}

	public static class ManhattanReducer
			extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values,
							Context context
							) throws IOException, InterruptedException {
			if(key.toString().equals("cost")){
				double cost = 0.0;
				for (Text val : values) {
					cost += Double.parseDouble(val.toString());
				}
				context.write(null, new Text( "cost " + String.valueOf(cost)));
			}
			else{
				double newCentroid[] = new double[d];
				int pointNum = 0;
				for (Text val : values) {
					String line = val.toString();
					String tokens[] = line.split("\\s+"); 
					for(int i=0; i<d; i++){
						newCentroid[i] += Double.parseDouble(tokens[i]);
					}
					pointNum += 1;					
				}
				String centroidStr = "";
				for(int i=0; i<d; i++){
					newCentroid[i] = newCentroid[i]/(double)pointNum;
					centroidStr += String.valueOf(newCentroid[i]);
					centroidStr += " ";
				}
				context.write(null, new Text(centroidStr));
			}
		}
	}

	public static class EuclideanDistanceMapper
        extends Mapper<Object, Text, Text, Text>{

		private Text word = new Text();		
		public Text testK = new Text();
		public double[][] center = new double[centerNum][d]; 
		
		///read center file
		public void setup(Context context) throws IOException, InterruptedException{
			URI[] files = context.getCacheFiles();
			URI uri = files[0];
			FileReader fr = new FileReader( new File(uri.getFragment()).toString() );
			BufferedReader bf = new BufferedReader(fr);
			int center_i = 0;
			String line;
			while(bf.ready()){
				line = bf.readLine().toString();
				String tokens[] = line.split("\\s+");
				if(tokens.length > 2){	/// not cost
					for(int i=0; i< tokens.length; i++){
						center[center_i][i] = Double.parseDouble(tokens[i]);
					}
					center_i += 1;
				}
			}
		}
		public void map(Object key, Text value, Context context
						) throws IOException, InterruptedException {
			String line = value.toString();
			String tokens[] = line.split("\\s+"); 
			for(int p1=0; p1<centerNum; p1++){
				for(int p2=p1+1; p2<centerNum; p2++){
					double sum = 0.0;
					double distance = 0.0;
					for(int i=0; i<d; i++){
						sum += Math.pow(center[p1][i]-center[p2][i], 2); 
					}
					distance = Math.sqrt(sum);
					Text k = new Text();
					Text v = new Text();
					k.set( String.valueOf(p1) + "-" + String.valueOf(p2) );
					v.set( String.valueOf(p1) + "-" + String.valueOf(p2)+ " "+ String.valueOf(distance));
					context.write(k, v);
				}
			}		
				
		}
	}
	public static class EuclideanDistanceReducer
			extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values,
							Context context
							) throws IOException, InterruptedException {
			Text v = new Text();
			for (Text val : values) {
				v = val;
			}
			context.write(null, v);			
		}
	}
	
	public static class ManhattanDistanceMapper
        extends Mapper<Object, Text, Text, Text>{

		private Text word = new Text();		
		public Text testK = new Text();
		public double[][] center = new double[centerNum][d]; 
		
		///read center file
		public void setup(Context context) throws IOException, InterruptedException{
			URI[] files = context.getCacheFiles();
			URI uri = files[0];
			FileReader fr = new FileReader( new File(uri.getFragment()).toString() );
			BufferedReader bf = new BufferedReader(fr);
			int center_i = 0;
			String line;
			while(bf.ready()){
				line = bf.readLine().toString();
				String tokens[] = line.split("\\s+");
				if(tokens.length > 2){	/// not cost
					for(int i=0; i< tokens.length; i++){
						center[center_i][i] = Double.parseDouble(tokens[i]);
					}
					center_i += 1;
				}
			}
		}
		public void map(Object key, Text value, Context context
						) throws IOException, InterruptedException {
			String line = value.toString();
			String tokens[] = line.split("\\s+"); 
			for(int p1=0; p1<centerNum; p1++){
				for(int p2=p1+1; p2<centerNum; p2++){
					double sum = 0.0;
					double distance = 0.0;
					for(int i=0; i<d; i++){
						sum += Math.abs(center[p1][i]-center[p2][i]); 
					}
					distance = sum;
					Text k = new Text();
					Text v = new Text();
					k.set( String.valueOf(p1) + "-" + String.valueOf(p2) );
					v.set( String.valueOf(p1) + "-" + String.valueOf(p2)+ " "+ String.valueOf(distance));
					context.write(k, v);
				}
			}		
				
		}
	}
	public static class ManhattanDistanceReducer
			extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values,
							Context context
							) throws IOException, InterruptedException {
			Text v = new Text();
			for (Text val : values) {
				v = val;
			}
			context.write(null, v);			
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		
		//  c1 Euclidean
		Job job = new Job(conf, "word count");
		job.addCacheFile( new URI( "data/" + otherArgs[0] + "#Euclidean"  )); // initial centers
		job.setJarByClass(Kmeans.class);
		job.setMapperClass(EuclideanMapper.class);
		job.setReducerClass(EuclideanReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/user/root/data/data.txt"));
		FileOutputFormat.setOutputPath(job, new Path("output/" +otherArgs[0] + "E1"));
		job.waitForCompletion(true);		
		int i=1;
		for(; i<20; i++){
			 job = new Job(conf, "word count");
			job.addCacheFile( new URI( "output/" +otherArgs[0] + "E" + i + "/part-r-00000" + "#Euclidean"  )); 
			job.setJarByClass(Kmeans.class);
			job.setMapperClass(EuclideanMapper.class);
			job.setReducerClass(EuclideanReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path("/user/root/data/data.txt"));
			FileOutputFormat.setOutputPath(job, new Path("output/" + otherArgs[0] + "E" + (i+1) ));
			job.waitForCompletion(true);
		}		
		// E dist
		job = new Job(conf, "word count");
		job.addCacheFile( new URI( "output/" + otherArgs[0] + "E" + (i) + "/part-r-00000#Euclidean"  ));
		job.setJarByClass(Kmeans.class);
		job.setMapperClass(EuclideanDistanceMapper.class);
		job.setReducerClass(EuclideanDistanceReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("data/" + otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path("output/" + otherArgs[0] + "E" + "_Edist"));
		job.waitForCompletion(true);
		
		// M dist
		job = new Job(conf, "word count");
		job.addCacheFile( new URI( "output/" + otherArgs[0] + "E" + (i) + "/part-r-00000#Manhattan"  ));
		job.setJarByClass(Kmeans.class);
		job.setMapperClass(ManhattanDistanceMapper.class);
		job.setReducerClass(ManhattanDistanceReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("data/" + otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path("output/" + otherArgs[0] + "E" + "_Mdist"));
		job.waitForCompletion(true);	
	
//============================================================================================
	  // c1 Manhattan
		job = new Job(conf, "word count");
		job.addCacheFile( new URI( "data/" + otherArgs[0] + "#Manhattan"  )); // initial centers
		job.setJarByClass(Kmeans.class);
		job.setMapperClass(ManhattanMapper.class);
		job.setReducerClass(ManhattanReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/user/root/data/data.txt"));
		FileOutputFormat.setOutputPath(job, new Path("output/" +otherArgs[0] + "M1"));
		job.waitForCompletion(true);		
		i=1;
		for(; i<20; i++){
			 job = new Job(conf, "word count");
			job.addCacheFile( new URI( "output/" +otherArgs[0] + "M" + i + "/part-r-00000" + "#Manhattan"  )); 
			job.setJarByClass(Kmeans.class);
			job.setMapperClass(ManhattanMapper.class);
			job.setReducerClass(ManhattanReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path("/user/root/data/data.txt"));
			FileOutputFormat.setOutputPath(job, new Path("output/" + otherArgs[0] + "M" + (i+1) ));
			job.waitForCompletion(true);
		}		
		// E dist
		job = new Job(conf, "word count");
		job.addCacheFile( new URI( "output/" + otherArgs[0] + "M" + (i) + "/part-r-00000#Manhattan"  ));
		job.setJarByClass(Kmeans.class);
		job.setMapperClass(EuclideanDistanceMapper.class);
		job.setReducerClass(EuclideanDistanceReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("data/" + otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path("output/" + otherArgs[0] + "M" + "_Edist"));
		job.waitForCompletion(true);
		
		// M dist
		job = new Job(conf, "word count");
		job.addCacheFile( new URI( "output/" + otherArgs[0] + "M" + (i) + "/part-r-00000#Manhattan"  ));
		job.setJarByClass(Kmeans.class);
		job.setMapperClass(ManhattanDistanceMapper.class);
		job.setReducerClass(ManhattanDistanceReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("data/" + otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path("output/" + otherArgs[0] + "M" + "_Mdist"));
		job.waitForCompletion(true);	
	
//>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
		//  c2 Euclidean
		job = new Job(conf, "word count");
		job.addCacheFile( new URI( "data/" + otherArgs[1] + "#Euclidean"  )); // initial centers
		job.setJarByClass(Kmeans.class);
		job.setMapperClass(EuclideanMapper.class);
		job.setReducerClass(EuclideanReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/user/root/data/data.txt"));
		FileOutputFormat.setOutputPath(job, new Path("output/" +otherArgs[1] + "E1"));
		job.waitForCompletion(true);		
		i=1;
		for(; i<20; i++){
			 job = new Job(conf, "word count");
			job.addCacheFile( new URI( "output/" +otherArgs[1] + "E" + i + "/part-r-00000" + "#Euclidean"  )); 
			job.setJarByClass(Kmeans.class);
			job.setMapperClass(EuclideanMapper.class);
			job.setReducerClass(EuclideanReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path("/user/root/data/data.txt"));
			FileOutputFormat.setOutputPath(job, new Path("output/" + otherArgs[1] + "E" + (i+1) ));
			job.waitForCompletion(true);
		}		
		// E dist
		job = new Job(conf, "word count");
		job.addCacheFile( new URI( "output/" + otherArgs[1] + "E" + (i) + "/part-r-00000#Euclidean"  ));
		job.setJarByClass(Kmeans.class);
		job.setMapperClass(EuclideanDistanceMapper.class);
		job.setReducerClass(EuclideanDistanceReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("data/" + otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path("output/" + otherArgs[1] + "E" + "_Edist"));
		job.waitForCompletion(true);
		
		// M dist
		job = new Job(conf, "word count");
		job.addCacheFile( new URI( "output/" + otherArgs[1] + "E" + (i) + "/part-r-00000#Manhattan"  ));
		job.setJarByClass(Kmeans.class);
		job.setMapperClass(ManhattanDistanceMapper.class);
		job.setReducerClass(ManhattanDistanceReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("data/" + otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path("output/" + otherArgs[1] + "E" + "_Mdist"));
		job.waitForCompletion(true);	
	
//============================================================================================
	  // c2 Manhattan
		job = new Job(conf, "word count");
		job.addCacheFile( new URI( "data/" + otherArgs[1] + "#Manhattan"  )); // initial centers
		job.setJarByClass(Kmeans.class);
		job.setMapperClass(ManhattanMapper.class);
		job.setReducerClass(ManhattanReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("/user/root/data/data.txt"));
		FileOutputFormat.setOutputPath(job, new Path("output/" +otherArgs[1] + "M1"));
		job.waitForCompletion(true);		
		i=1;
		for(; i<20; i++){
			 job = new Job(conf, "word count");
			job.addCacheFile( new URI( "output/" +otherArgs[1] + "M" + i + "/part-r-00000" + "#Manhattan"  )); 
			job.setJarByClass(Kmeans.class);
			job.setMapperClass(ManhattanMapper.class);
			job.setReducerClass(ManhattanReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path("/user/root/data/data.txt"));
			FileOutputFormat.setOutputPath(job, new Path("output/" + otherArgs[1] + "M" + (i+1) ));
			job.waitForCompletion(true);
		}		
		// E dist
		job = new Job(conf, "word count");		
		job.addCacheFile( new URI( "output/" + otherArgs[1] + "M" + (i) + "/part-r-00000#Manhattan"  ));
		job.setJarByClass(Kmeans.class);
		job.setMapperClass(EuclideanDistanceMapper.class);
		job.setReducerClass(EuclideanDistanceReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("data/" + otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path("output/" + otherArgs[1] + "M" + "_Edist"));
		job.waitForCompletion(true);
		
		// M dist
		job = new Job(conf, "word count");
		job.addCacheFile( new URI( "output/" + otherArgs[1] + "M" + (i) + "/part-r-00000#Manhattan"  ));
		job.setJarByClass(Kmeans.class);
		job.setMapperClass(ManhattanDistanceMapper.class);
		job.setReducerClass(ManhattanDistanceReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("data/" + otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path("output/" + otherArgs[1] + "M" + "_Mdist"));
		job.waitForCompletion(true);
	}
}