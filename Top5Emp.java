import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Top5Emp extends Configured implements Tool {
	private static int N=5;
	static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
		
		private TreeMap<Integer,Text> top5= new TreeMap<Integer,Text>();
		public void map(LongWritable key,Text Value,Context context) throws IOException, InterruptedException
		{
			String str[]=Value.toString().split(",");
			int num=0;
			num=Integer.parseInt(str[33]);
			Text val=new Text(str[9]);
			top5.put(num,val);
			if(top5.size()>N){
			top5.remove(top5.firstKey());
			}
		}
		protected void cleanup(Context context) throws IOException, InterruptedException{
			for(Integer k:top5.keySet()){
			context.write(new Text(k.toString()),top5.get(k));
			
		}
	}
	}
	
	 
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		private static final TreeMap<Text,Text> top5=new TreeMap<Text, Text>();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
				
					for(Text value: values) 
					{
				
						top5.put(new Text(key), new Text(value));			
						if(top5.size()>N)
					    {
						top5.remove(top5.firstKey());//removes the first (lowest) key in the map
					    }
				    }
		}
		protected void cleanup(Context context) throws IOException, InterruptedException{
	for(Text t:top5.descendingKeySet()){
		context.write(t,top5.get(t));
	}
	}
	}
	
	public int run (String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			// TODO Auto-generated method stub
			Configuration conf = getConf();
			//conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
			
			Job job = new Job(conf,"Top5Emp");
			
			
			job.setJarByClass(Top5Emp.class);
			
			
			
			job.setNumReduceTasks(1);
			
			job.setMapperClass(MyMapper.class);
			//job.setCombinerClass(MyReducer.class); //Added the combiner class
			job.setReducerClass(MyReducer.class);
			//job.setSortComparatorClass(Comparator.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			
			
			job.setOutputFormatClass(TextOutputFormat.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			
			System.exit(job.waitForCompletion(true)? 0:1);
			return 0;
		
	}

public static void main(String[] args) throws Exception {
	int res=ToolRunner.run(new Configuration(), new Top5Emp(),args);
	System.exit(res);
}

}


	
		








