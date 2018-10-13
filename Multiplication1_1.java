import java.io.IOException;
import java.util.StringTokenizer;
import java.util.LinkedList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Multiplication1_1 {

	// Complete the Matrix1_1_Mapper class. 
	// Definitely, Generic type (Text, Text, Text, Text) must not be modified
	
	// Optional, you can add and use new methods in this class
	// Optional, you can use both 'setup' and 'cleanup' function, or either of them, or none of them.
	
	public static class Matrix1_1_Mapper extends Mapper<Text, Text, Text, Text> {
		// Optional, Using, Adding, Modifying and Deleting variable is up to you
		int n_first_rows = 0;
		int n_first_cols = 0;
		int n_second_cols = 0;
		private String newEntry = null;
		
		// Optional, Utilizing 'setup' function or not is up to you
		protected void setup(Context context) throws IOException, InterruptedException {
			n_first_rows = context.getConfiguration().getInt("n_first_rows", 0);
			n_first_cols = context.getConfiguration().getInt("n_first_cols", 0);
			n_second_cols = context.getConfiguration().getInt("n_second_cols", 0);
		}
				
		// Definitely, parameter type and name (Text matrix, Text entry, Context context) must not be modified
		public void map(Text matrix, Text entry, Context context) throws IOException, InterruptedException {
			// Implement map function.
			String[] entries = entry.toString().split(",");
			if(matrix.toString().equals("a"))
			{
				String newval = matrix.toString()+","+entries[2]+","+entries[1];
				for(int i=0; i<n_second_cols;i++)
				{
					String newkey = entries[0]+","+String.valueOf(i);
					context.write(new Text(newkey), new Text(newval));
				}
			}
			else if(matrix.toString().equals("b"))
			{
				String newval = matrix.toString()+","+entries[2]+","+entries[0];
				for(int i=0; i< n_first_rows;i++)
				{
					String newkey = String.valueOf(i)+","+entries[1];
					context.write(new Text(newkey), new Text(newval));
				}
			}
		}
	}

	// Complete the Matrix1_1_Reducer class. 	
	// Definitely, Generic type (Text, Text, Text, Text) must not be modified
	// Definitely, Output format and values must be the same as given sample output 
	
	// Optional, you can use both 'setup' and 'cleanup' function, or either of them, or none of them.
	// Optional, you can add and use new methods in this class
	public static class Matrix1_1_Reducer extends Reducer<Text, Text, Text, Text> {
		// Optional, Using, Adding, Modifying and Deleting variable is up to you
		int n_first_rows = 0;
		int n_first_cols = 0;
		int n_second_cols = 0;
		
		// Optional, Utilizing 'setup' function or not is up to you
		protected void setup(Context context) throws IOException, InterruptedException {
			n_first_rows = context.getConfiguration().getInt("n_first_rows", 0);
			n_first_cols = context.getConfiguration().getInt("n_first_cols", 0);
			n_second_cols = context.getConfiguration().getInt("n_second_cols", 0);
		}
		
		// Definitely, parameters type (Text, Iterable<Text>, Context) must not be modified
		// Optional, parameters name (key, values, context) can be modified
		public void reduce(Text entry, Iterable<Text> entryComponents, Context context) throws IOException, InterruptedException {
		  // Implement reduce function.
		  LinkedList<String> entries1 = new LinkedList<String>();
		  LinkedList<String> entries2 = new LinkedList<String>();		
		  for(Text records : entryComponents)
		  {
		    entries1.add(records.toString());
		    entries2.add(records.toString());
		  }
		  int sum = 0;
		  for(String entry1 : entries1)
		  {
		    String[] components1 = entry1.split(",");
		    if(components1[0].equals("a"))
		    {
		      for(String entry2 : entries2)
		      {
			String[] components2 = entry2.split(",");
			if(components2[0].equals("b"))
			{
			  if(components1[2].equals(components2[2]))
			  {
			    sum = sum + Integer.parseInt(components1[1])*Integer.parseInt(components2[1]);
			  }
			}			
		      }
		    }
		  }
		  context.write(entry, new Text(String.valueOf(sum)));
		}
	}
	
	// Definitely, Main function must not be modified 
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Matrix Multiplication1_1");

		job.setJarByClass(Multiplication1_1.class);
		job.setMapperClass(Matrix1_1_Mapper.class);
		job.setReducerClass(Matrix1_1_Reducer.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.getConfiguration().setInt("n_first_rows", Integer.parseInt(args[2]));
		job.getConfiguration().setInt("n_first_cols", Integer.parseInt(args[3]));
		job.getConfiguration().setInt("n_second_cols", Integer.parseInt(args[4]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
