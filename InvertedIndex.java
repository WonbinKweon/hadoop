import java.io.IOException;
import java.util.StringTokenizer;
import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.Text;

public class InvertedIndex {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text word = new Text();
    private String prettyword = new String();
    private String fileName = new String(); 
  
    public void setup(Context context)
      throws java.io.IOException, InterruptedException   
    {
      Path filePath = ((FileSplit) context.getInputSplit()).getPath();
      fileName = filePath.getName();
    }       

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        prettyword = word.toString();
        prettyword = prettyword.replace(".", "");
        word.set(prettyword);
        context.write(word, new Text(fileName));
      }
    }
  }

  public static class myReducer
       extends Reducer<Text,Text,Text,Text>{

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      StringBuilder stringBuilder = new StringBuilder();
      for(Text value : values)
      {
        stringBuilder.append(value.toString());
        if(values.iterator().hasNext())
        {
          stringBuilder.append(",");
        }
      }
      context.write(key, new Text(stringBuilder.toString()));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "InvertedIndex");
    job.setJarByClass(InvertedIndex.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(myReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileSystem fs = FileSystem.get(conf);
    Path mypath = new Path(args[0]);
    RemoteIterator<LocatedFileStatus> itr2  = fs.listFiles(mypath, true);    
    while(itr2.hasNext())
    {
      LocatedFileStatus fileStatus = itr2.next();
      FileInputFormat.addInputPath(job, fileStatus.getPath());
    }
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
