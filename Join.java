import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.LinkedList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Join {
	// Complete the JoinMapper class. 
	// Definitely, Generic type (LongWritable, Text, Text, Text) must not be modified
	public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {
		// You have to use these variables to generalize the Join.
		String[] tableNames = new String[2];
		String first_table_name = null;
		int first_table_join_index;
		String second_table_name = null;
		int second_table_join_index;
		
		protected void setup(Context context) throws IOException, InterruptedException {
			// Don't change setup function
			tableNames = context.getConfiguration().getStrings("table_names");
			first_table_join_index = context.getConfiguration().getInt("first_table_join_index", 0);
			second_table_join_index = context.getConfiguration().getInt("second_table_join_index", 0);
			first_table_name = tableNames[0];
			second_table_name = tableNames[1];
		}
		
		public void map(LongWritable key, Text record, Context context) throws IOException, InterruptedException {
			 //Implement map function
                        String[] features = record.toString().split(",");
			Text newKey = new Text();
			if(features[0].equals("\""+first_table_name+"\"")){
			  newKey = new Text(features[first_table_join_index]);
				}
			else{
			  newKey = new Text(features[second_table_join_index]);
				}
		        context.write(newKey, record);
			}
	}

	// Don't change (key, value) types
	public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
		String[] tableNames = new String[2];
		String first_table_name = null;
		String second_table_name = null;
		
		protected void setup(Context context) throws IOException, InterruptedException {
			// Similar to Mapper Class
			tableNames = context.getConfiguration().getStrings("table_names");
			first_table_name = tableNames[0];
			second_table_name = tableNames[1];
		}
		
		public void reduce(Text order_id, Iterable<Text> records, Context context) throws IOException, InterruptedException {
			// Implement reduce function
			// You can see form of new (key, value) pair in sample output file on server.
			LinkedList<String> recordslist = new LinkedList<String>();
			LinkedList<String> recordslist2 = new LinkedList<String>();
			Text pretty_order_id = new Text(order_id.toString().replace("\"","" ));
			for(Text record : records)
			{
			  recordslist.add(record.toString());
			  recordslist2.add(record.toString());
			}
	
			for(String record1 : recordslist)
			{
			  String[] features = record1.split(",");
			  if(features[0].replace("\"", "").equals(first_table_name))
			  {
			    String originvalue = record1 + ",";
			    for(String record2 : recordslist2)
			    {
			      String[] features_ = record2.split(",");
			      if(features_[0].replace("\"", "").equals(second_table_name))
			      {
				String newvalue = originvalue + record2;
				context.write(pretty_order_id, new Text(newvalue));
			      }	
			    }
			  } 
			}
			// You can use Array or List or other Data structure for 'cache'.
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Join");

		job.setJarByClass(Join.class);
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReducer.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	    
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.getConfiguration().setStrings("table_names", args[2]);
		job.getConfiguration().setInt("first_table_join_index", Integer.parseInt(args[3]));
		job.getConfiguration().setInt("second_table_join_index", Integer.parseInt(args[4]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
