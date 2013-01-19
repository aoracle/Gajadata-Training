
import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class WordCountSearch {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String inputword = conf.get("searchword");
        System.out.println("Input Search Word:"+inputword);

        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            System.out.println("Word:"+word.toString());
            System.out.println("Input Word:"+inputword);
            if(!(inputword == null) && !(inputword.equals("ZZZZ"))) {
                if(inputword.equals(word.toString())) {
            	    context.write(word, one);
	        }
	    } else {
            	context.write(word, one);
	        System.out.println("Map element:"+context);
	    }
        }
    }
 } 
        
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String inputword;
    String inputpath;
    String outputpath;
        
    System.out.println("Input Arguments");
    for(int i=0;i<args.length;i++){
       System.out.println("Argument "+i+":"+args[i]);
    }

    if(args.length == 3){
       inputword = args[0];
       inputpath = args[1];
       outputpath = args[2];
       conf.set("searchword",inputword);
       System.out.println("Conf get searchword:"+conf.get("searchword"));
    } else {
       inputword = "No Input Search Word. Counting all words.";
       inputpath = args[0];
       outputpath = args[1];
       conf.set("searchword","ZZZZ");
    }

    System.out.println("Search word:"+inputword);
    System.out.println("Input Path:"+inputpath);
    System.out.println("Output Path:"+outputpath);
  
    Job job = new Job(conf, "wordcountsearch");
    FileInputFormat.addInputPath(job, new Path(inputpath));
    FileOutputFormat.setOutputPath(job, new Path(outputpath));

    job.setJarByClass(WordCountSearch.class); 

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
 
    /*if(args[0].startsWith("word=")){
       inputword = args[0];
       FileInputFormat.addInputPath(job, new Path(args[1]));
       FileOutputFormat.setOutputPath(job, new Path(args[2]));
    } else {
       FileInputFormat.addInputPath(job, new Path(args[0]));
       FileOutputFormat.setOutputPath(job, new Path(args[1]));
    }*/
     
    job.waitForCompletion(true);
 }
        
}
