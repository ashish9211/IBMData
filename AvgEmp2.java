
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


public class AvgEmp2 {
	
	static class MyMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
		public void map(LongWritable key,Text Value,Context context) throws IOException, InterruptedException
		{
			
			
			String str[]=Value.toString().split(",");
			
			
			context.write(new Text(str[15]),new IntWritable(Integer.parseInt(str[18])));
			
		}
	}
	 
	static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		public void reduce (Text key,Iterable<IntWritable> value,Context context)
		{
			int avg=0;
			int sum=0;
			int count=0;
		
			for(IntWritable f:value){
				sum+=f.get();
				count++;
			}
			 avg=(int)(sum/count);
			
			
			 for(IntWritable f1:value){
					if(avg==f1.get()||avg==((int)(f1.get())+1)||avg==((int)(f1.get())-1)){
					
					
			try {
				
				
				
				context.write(key,new IntWritable(avg));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
				
			}}
		}
			
	}
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
	Configuration conf =new Configuration();
	Job job=Job.getInstance(conf, "AvgSal per Dept");
	job.setJarByClass(AvgEmp2.class);
	job.setMapperClass(MyMapper.class);
	
	//job.setCombinerClass(MyReducer.class);
	job.setReducerClass(MyReducer.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	FileInputFormat.addInputPath(job,new Path(args[0]));
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	System.exit(job.waitForCompletion(true)? 0:1);
	}
}