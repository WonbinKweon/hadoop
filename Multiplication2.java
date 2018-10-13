import java.io.IOException;
import java.util.StringTokenizer;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Multiplication2 {
	// Complete the Matrix2_Mapper class. 
	// Definitely, Generic type (Text, Text, Text, Text) must not be modified
	
	// Optional, you can use both 'setup' and 'cleanup' function, or either of them, or none of them.
	// Optional, you can add and use new methods in this class

	public static class Matrix2_Mapper extends Mapper<Text, Text, Text, Text> {
		// Optional, Using, Adding, Modifying and Deleting variable is up to you
		int result_rows = 0;
		int result_columns = 0;
		int n_first_cols = 0;
		int n_second_cols = 0;
		private String newEntry = null;

		// Optional, Utilizing 'setup' function or not is up to you
		protected void setup(Context context) throws IOException, InterruptedException {
			result_rows = context.getConfiguration().getInt("n_first_rows", 0);
			n_first_cols = context.getConfiguration().getInt("n_first_cols", 0);
			n_second_cols = context.getConfiguration().getInt("n_second_cols", 0);
			result_columns = context.getConfiguration().getInt("n_third_cols", 0);
		}

		// Definitely, parameter type and name (Text matrix, Text entry, Context context) must not be modified
		public void map(Text matrix, Text entry, Context context) throws IOException, InterruptedException {
		  // Implement map function.
		  String[] entries = entry.toString().split(",");
		  if(matrix.toString().equals("a"))
		  {
		    for(int i=0; i<n_second_cols; i++)
		    {
		      String newval=matrix.toString()+","+entries[2]+","+entries[1]+","+String.valueOf(i);
		      for(int j=0; j<result_columns; j++)
		      {
			String newkey = entries[0]+","+String.valueOf(j);
			context.write(new Text(newkey), new Text(newval));
		      }
		    }
		  }
		  else if(matrix.toString().equals("b"))
		  {
		    for(int i=0;i<result_rows; i++)
		    {
		      for(int j=0; j<result_columns; j++)
		      {
			String newkey = String.valueOf(i)+","+String.valueOf(j);
			String newval = matrix.toString()+","+entries[2]+","+entries[0]+","+entries[1];
			context.write(new Text(newkey), new Text(newval));
	  	      }
		    }
		  }
		  else if(matrix.toString().equals("c"))
		  {
		    String newval = matrix.toString()+","+entries[2]+","+entries[0];
		    for(int i=0;i<result_rows;i++)
		    {
		      String newkey = String.valueOf(i)+","+entries[1];
		      context.write(new Text(newkey), new Text(newval));
		    }
		  }
		}
	}


	// Complete the Matrix2_Reducer class. 	
	// Definitely, Generic type (Text, Text, Text, Text) must not be modified
	// Definitely, Output format and values must be the same as given sample output 
	
	// Optional, you can use both 'setup' and 'cleanup' function, or either of them, or none of them.
	// Optional, you can add and use new methods in this class
	public static class Matrix2_Reducer extends Reducer<Text, Text, Text, Text> {
		// Optional, Using, Adding, Modifying and Deleting variable is up to you
		int result_rows = 0;
		int result_columns = 0;
		int n_first_cols = 0;
		int n_second_cols = 0;

		// Optional, Utilizing 'setup' function or not is up to you
		protected void setup(Context context) throws IOException, InterruptedException {
			result_rows = context.getConfiguration().getInt("n_first_rows", 0);
			n_first_cols = context.getConfiguration().getInt("n_first_cols", 0);
			n_second_cols = context.getConfiguration().getInt("n_second_cols", 0);
			result_columns = context.getConfiguration().getInt("n_third_cols", 0);
		}

		// Definitely, parameters type (Text, Iterable<Text>, Context) must not be modified		
		// Optional, parameters name (key, values, context) can be modified
		public void reduce(Text entry, Iterable<Text> entryComponents, Context context) throws IOException, InterruptedException {
		  // Implement reduce function.
		  LinkedList<String> entry1 = new LinkedList<String>();
		  LinkedList<String> entry2 = new LinkedList<String>();
		  LinkedList<String> entry3 = new LinkedList<String>();
		  for(Text record : entryComponents)
		  {
		    entry1.add(record.toString());
		    entry2.add(record.toString());
		    entry3.add(record.toString());
		  } 
		  int sum = 0; 
		  for(String record1 : entry1)
		  {
		    String[] entries1 = record1.split(",");
		    if(entries1[0].equals("c"))
		    {
		      int middle = 0;
		      for(String record2 : entry2)
		      {
			String[] entries2 = record2.split(",");
			if(entries2[0].equals("a") && entries2[3].equals(entries1[2]))
			{
			  for(String record3 : entry3)
			  {
			    String[] entries3 = record3.split(",");
			    if(entries3[0].equals("b") && entries2[2].equals(entries3[2]) && entries3[3].equals(entries1[2]))
			    {
			      middle = middle + Integer.parseInt(entries2[1])*Integer.parseInt(entries3[1]);
			    }
			  }
			}
		      }
		      sum = sum + middle*Integer.parseInt(entries1[1]);
		    }
		  }
		  context.write(entry, new Text(String.valueOf(sum)));

		}
	}

	// Definitely, Main function must not be modified 
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Matrix Multiplication");

		job.setJarByClass(Multiplication2.class);
		job.setMapperClass(Matrix2_Mapper.class);
		job.setReducerClass(Matrix2_Reducer.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.getConfiguration().setInt("n_first_rows", Integer.parseInt(args[2]));
		job.getConfiguration().setInt("n_first_cols", Integer.parseInt(args[3]));
		job.getConfiguration().setInt("n_second_cols", Integer.parseInt(args[4]));
		job.getConfiguration().setInt("n_third_cols", Integer.parseInt(args[5]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
