package org.apache.hadoop.examples;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.Arrays;
import java.util.HashMap;

public class PageRank {
	private static int N = 10879;
    private static float d = 0.8f;
	private static float[] M = new float[N];
	//private static int[] nodes = new int[10];
	
	private static int[] exist = new int[N];
	private static float[] initP = new float[N];
	//private static float nodesNum;
	
	/// 看誰存在
	public static class InitialPageRankMapper
        extends Mapper<Object, Text, Text, Text>{
		
		private Text k = new Text();
		private Text v = new Text();

		public void map(Object key, Text value, Context context
						) throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] words = line.split("	");
			
			k.set("1");
			v.set(words[0]);
			context.write(k, v);
			
			k.set("1");
			v.set(words[1]);////////*********must test*******************			
			context.write(k, v);			
		}
	}

	public static class InitialPageRankReducer
			extends Reducer<Text,Text,Text,Text> {
		private Text v = new Text();		

		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException
		{
			int[] e = new int[N];
			float nodesNum = 0;
			
			for(Text val:values){
				int idx = Integer.parseInt(val.toString());
				e[idx] = 1;
			}
			for(int i=0; i<N; i++){
				if(e[i]==1){
					nodesNum += 1;
				}
			}
			for(int i=0; i<N; i++){
				if(e[i]==1){
					v.set( Integer.toString(i) +" P " + Float.toString(1/nodesNum) );
					context.write(null, v);
				}				
			}
		}
	}
	
	
    public static class AdjacencyMatrixMapper
        extends Mapper<Object, Text, Text, Text>{

		private Text end = new Text();
		private Text start = new Text();

		public void map(Object key, Text value, Context context
						) throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] words = line.split("	");
		
			start.set(words[0]);
			end.set(words[1]);
			context.write(start, end);
		}
	}

	public static class AdjacencyMatrixReducer
			extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();		

		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException
		{
			
			Arrays.fill(M, (float) ((1 - d) / N));	
			float[] A = new float[N+10];
			
			int sum = 0;
			for(Text val : values) {
				int index = Integer.parseInt(val.toString());
				A[index] = 1;
				sum += 1;
			}
			if (sum == 0) {// 分母不能為0
                sum = 1;
            }
			
			StringBuilder sb = new StringBuilder();
            for (int i = 0; i < N; i++) {
                sb.append(" " + (float) (M[i] + d * A[i] / sum));
            }
			
		   Text sum_text = new Text(String.valueOf(sum));
		   
		   //Text v = new Text(sb.toString().substring(1));
		   Text v = new Text( key.toString() + " M " + sb.toString().substring(1));		   
		   result.set(v);
		   //Text k = new Text(key.toString() + " M");
		   context.write(null,result);
		}
	}

	public static class MultiplyMapper
        extends Mapper<Object, Text, Text, Text>{

		private Text k = new Text();
		private Text v = new Text();

		public void map(Object key, Text value, Context context
						) throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] words = line.split(" ");
			
			if(words[1].equals("M"))
			{
				int end = 0; 
				while (end < N) {
					String start = words[0];
					k.set(Integer.toString(end));
					String p = words[end+2]; ///到第i個的機率
					v.set( "M " + start + " " + p );
					context.write(k, v);
					end += 1;
				}
			}
			else if(words[1].equals("P"))
			{
				int end = 0; 
				while (end < N) {
					k.set(Integer.toString(end));
					v.set("P " + words[0] + " " + words[2]);
					context.write(k, v);
					end += 1;
				}
			}
			
		}
	}

	public static class MultiplyReducer
			extends Reducer<Text,Text,Text,Text> {

		public void reduce(Text key, Iterable<Text> values,
							Context context
							) throws IOException, InterruptedException {
								
			HashMap<Integer, Float> hashM = new HashMap<Integer, Float>();
			HashMap<Integer, Float> hashP = new HashMap<Integer, Float>();
//			/*
			for(Text val : values) {
				String line = val.toString();
				String[] words = line.split(" ");
				if(words[0].equals("M"))
				{
				   hashM.put( Integer.parseInt(words[1]), Float.parseFloat(words[2]) ); // 從哪來的，機率**
				   /*
				   Text v = new Text(line );
				   context.write(key, v);
				   */
				}
				else if(words[0].equals("P"))
				{
					hashP.put( Integer.parseInt( words[1] ), Float.parseFloat(words[2]) );// **
					/*
					Text v = new Text(line);
				   context.write(key, v);
				   */
				}				
			}
			
			float sum = 0;
			float m, p; 
			for(int i=0; i<N; i++){
				m = hashM.containsKey(i) ? hashM.get(i) : 0;
				p = hashP.containsKey(i) ? hashP.get(i) : 0;
				sum += m * p;
			}
			
			Text v = new Text(key.toString() + " P " + Float.toString(sum));
			context.write(null, v);	
//			*/
		}
	}
	
	
	public static class NormalizeMapper
        extends Mapper<Object, Text, Text, Text>{

		private Text k = new Text();
		private Text v = new Text();

		public void map(Object key, Text value, Context context
						) throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] words = line.split(" ");
			
			k.set("1");
			v.set(words[0] + " " + words[2]);
			//v.set(line);
			context.write(k, v);	
		}
	}

	public static class NormalizeReducer
			extends Reducer<Text,Text,Text,Text> {
		private Text v = new Text();		

		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException
		{
			float[] newP = new float[N];
			float sum = 0.0f;
			int idx = 0;
			for(Text val:values){
				
				String line = val.toString();
				String[] words = line.split(" ");
				
				float p = Float.parseFloat(words[1].toString());
				idx = Integer.parseInt(words[0]);
				newP[idx] = p;
				sum += p;
				
			}
			float left = (float)(1.0-sum)/(float)N;
			
			for(int i=0; i<N; i++){
				newP[i] += left;
				v.set( Integer.toString(i) + " P " + Float.toString(newP[i]) );
				context.write(null, v);
			}
			
		}
	}
	
	public static class FindTopMapper
        extends Mapper<Object, Text, Text, Text>{

		private Text k = new Text();
		private Text v = new Text();

		public void map(Object key, Text value, Context context
						) throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] words = line.split(" ");
			
			k.set("1");
			v.set(words[0] + " " + words[2]);
			//v.set(line);
			context.write(k, v);	
		}
	}

	public static class FindTopReducer
			extends Reducer<Text,Text,Text,Text> {
		private Text v = new Text();		

		public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException
		{
			int[] topNode = new int[10];
			float[] topP = new float[10];
			
			int temp = -1;
			float temp_v = 0.0f;			
			
			// ***** 10/3 *****
			for(int i=0; i<10; ++i){
				topNode[i] = -1;
				topP[i] = 0;					
			}				
			// ***** 10/3 *****
			
			
			for(Text val:values){
				
				String line = val.toString();
				String[] words = line.split(" ");
				
				float p = Float.parseFloat(words[1].toString());
				int idx = Integer.parseInt(words[0]);
				
				for(int i=0; i<10; ++i){
					if(topP[i] < p){ // 當前的 node 比之前的 top10 之一大
						// 被換下來的要在之後與其他 top10 比
						temp = topNode[i];
						temp_v = topP[i];
						
						topNode[i] = idx;
						topP[i] = p;
						
						idx = temp;
						p = temp_v;						
					}
				}	
				
				
				/*				
				for(int i=0; i<10; i++){ //******************
					if(topP[i] < p){
						topNode[i] = idx;
						topP[i] = p;
						break;
					}
				}
				*/
			}
			for(int i=0; i<10; i++){ //********************
				Text v = new Text(Integer.toString(topNode[i]) + " " + Float.toString(topP[i]));
				context.write(null, v);
			}			
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: PageRank <in> <out>");
			System.exit(2);
		}
		
		/// initial PR
		Job job = new Job(conf, "initialPR");
		
		job.setJarByClass(PageRank.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(InitialPageRankMapper.class);
		//job.setCombinerClass(AdjacencyMatrixReducer.class);
		job.setReducerClass(InitialPageRankReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]+"no0"));
		//System.exit
		job.waitForCompletion(true);
		
		
		
		/// adjacency matrix
		job = new Job(conf, "adjMatrix");
		job.setJarByClass(PageRank.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(AdjacencyMatrixMapper.class);
		//job.setCombinerClass(AdjacencyMatrixReducer.class);
		job.setReducerClass(AdjacencyMatrixReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]+"_M"));
		//System.exit
		job.waitForCompletion(true);
		
		
		
		/// page rank
		int i=0;
		for( i=0; i<20; i++){
			
			job = new Job(conf, "WordCount");
			//job = new Job(conf, "WordCount");
			job.setJarByClass(PageRank.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			job.setMapperClass(MultiplyMapper.class);
			job.setReducerClass(MultiplyReducer.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			FileInputFormat.addInputPath(job, new Path(otherArgs[1]+"_M"));
			//FileInputFormat.addInputPath(job,  new Path("/user/root/output/out72_M"));
			//FileInputFormat.addInputPath(job,  new Path(otherArgs[0]));
			
			FileInputFormat.addInputPath(job,  new Path(otherArgs[1] + "no" + i));
			//FileInputFormat.addInputPath(job,  new Path("/user/root/output/out72pr0"));
			
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + "pr" + (i+1)));
			//FileOutputFormat.setOutputPath(job, new Path("/user/root/data/pr"+(i+1)+".txt"));
			
			//System.exit(job.waitForCompletion(true) ? 0 : 1);
			job.waitForCompletion(true);
			
		//=======================================================================
		
			/// normalize   
			job = new Job(conf, "normalize");
			job.setJarByClass(PageRank.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setMapperClass(NormalizeMapper.class);
			job.setReducerClass(NormalizeReducer.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			//FileInputFormat.addInputPath(job, new Path("/user/root/output/out90pr1"));
			FileInputFormat.addInputPath(job, new Path(otherArgs[1] + "pr" + (i+1)));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + "no" + (i+1)));
			//System.exit
			job.waitForCompletion(true);			
		}
		
		
//		/*
		/// find top10   
		job = new Job(conf, "top10");
		job.setJarByClass(PageRank.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(FindTopMapper.class);
		job.setReducerClass(FindTopReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1] + "no" + (i)));
		//FileInputFormat.addInputPath(job, new Path("/user/root/output/out1no1"));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + "top10" ));
		//System.exit
		job.waitForCompletion(true); 
//		*/
		
	}
}