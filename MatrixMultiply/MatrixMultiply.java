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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.HashMap;

class Map
    extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>
{
    @Override
    public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException
    {
        int m = 500;	/// change input size here
        int p = 500;	/// change input size here
        String line = value.toString();
        String[] words = line.split(",");
        Text outputKey = new Text();
        Text outputValue = new Text();
        if (words[0].equals("M"))
        {
            for (int k = 0; k < p; k++)
            {
                outputKey.set(words[1] + "," + k);
                outputValue.set(words[0] + "," + words[2] + "," + words[3]);
                context.write(outputKey, outputValue);
            }
        }
        else {
            for (int i = 0; i < m; i++)
            {
                outputKey.set(i + "," + words[2]);
                outputValue.set("N," + words[1] + "," + words[3]);
                context.write(outputKey, outputValue);
            }
        }
    }
}

class Reduce
    extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>
{
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException
    {
        String[] value;
        HashMap<Integer, Integer> hashM = new HashMap<Integer, Integer>();
        HashMap<Integer, Integer> hashN = new HashMap<Integer, Integer>();
        for (Text val : values)
        {
            value = val.toString().split(",");
            if (value[0].equals("M"))
            {
                hashM.put(Integer.parseInt(value[1]), Integer.parseInt(value[2]));
            }
            else
            {
                hashN.put(Integer.parseInt(value[1]), Integer.parseInt(value[2]));
            }
        }
      
        int n = 500;	/// change input size here
        int sum = 0;
        int m_ij;
        int n_jk;
        for (int j = 0; j < n; j++)
        {
            m_ij = hashM.containsKey(j) ? hashM.get(j) : 0;
            n_jk = hashN.containsKey(j) ? hashN.get(j) : 0;
            sum += m_ij * n_jk;
        }

        Text out = new Text(key.toString() + "," + Integer.toString(sum));
        context.write(null,	out);

    }
}

public class MatrixMultiply
{

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length != 2)
        {
            System.err.println("Usage: MatrixMultiply <in> <out>");
            System.exit(2);
        }
        
        Job job = new Job(conf, "MatrixMultiply");
        job.setJarByClass(MatrixMultiply.class);
		    job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);		
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);     
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.waitForCompletion(true);
    }
}
