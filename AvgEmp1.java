
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AvgEmp1 {
	
	static class MyMapper extends Mapper<LongWritable,Text,Text,FloatWritable>{
		public void map(LongWritable key,Text Value,Context context) throws IOException, InterruptedException
		{
			
			
			String str[]=Value.toString().split(",");
			
			
			context.write(new Text(str[15]),new FloatWritable(Float.parseFloat(str[18])));
			
		}
	}
	 
	static class MyReducer extends Reducer<Text,FloatWritable,Text,FloatWritable>
	{
		public void reduce (Text key,Iterable<FloatWritable> value,Context context)
		{
			float avg=0;
			float sum=0;
			int count=0;
		
			for(FloatWritable f:value){
				sum+=f.get();
				count++;
			}
			avg=sum/count;
			
			try {
				
				context.write(key,new FloatWritable(avg));
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
	Job job=Job.getInstance(conf, "AvgSal per Dept");
	job.setJarByClass(AvgEmp1.class);
	job.setMapperClass(MyMapper.class);
	job.setReducerClass(MyReducer.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(FloatWritable.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(FloatWritable.class);
	FileInputFormat.addInputPath(job,new Path(args[0]));
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	System.exit(job.waitForCompletion(true)? 0:1);
	}
}