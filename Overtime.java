
import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Overtime {
	
	static class MyMapper extends Mapper<LongWritable,Text,IntWritable,Text>{
		public void map(LongWritable key,Text Value,Context context) throws IOException, InterruptedException
		{
			String str[]=Value.toString().split(",");
			if(str[22].equalsIgnoreCase("Yes"))
			{			
			context.write(new IntWritable(Integer.parseInt(str[9])),new Text(str[4]));
			}
		}
	}
	 
	static class MyReducer extends Reducer<IntWritable,Text,IntWritable,Text>
	{
		public void reduce (IntWritable key,Text value,Context context)
		{
			
			try {
				context.write(key,value);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
	Configuration conf =new Configuration();
	Job job=Job.getInstance(conf, "Overtime");
	job.setJarByClass(Overtime.class);
	job.setMapperClass(MyMapper.class);
	job.setReducerClass(MyReducer.class);
	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(Text.class);
	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(Text.class);
	FileInputFormat.addInputPath(job,new Path(args[0]));
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	System.exit(job.waitForCompletion(true)? 0:1);
	}
}