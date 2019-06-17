import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class exercise2 extends Configured implements Tool {
	
                                                                    //Mapper Input Key: Byte Offset of Line (IntWritable)
                                                                    //Mapper Input Value: line of file (Text)
                                                                    //Mapper Output Key: Word (Text)
                                                                    //Mapper Output Value: 1 (IntWritable)
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable> {
		
        //Declare mapper output variables outside of map function
        //If you were mapping one billion rows, this operation would still be completed once per initiation,
        	//rather than once per row
		//private static final int one = new IntWritable(1);
    	//private Text word = new Text();

		public void configure(JobConf job) {
		}//configure
		protected void setup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
			//The setup job is ran once in each map task. Here you could initialize a set of words which
			//you want to exclude from word count, like a set of stop words.
				// example: https://stackoverflow.com/questions/25432598/what-is-the-mapper-of-reducer-setup-used-for
		}//setup
                    //  Byte Offset of Line, line text                Word      1
		public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
		    //Read first value (line)
		    String line = value.toString();
		    List<String> ibm = new ArrayList<String>(Arrays.asList(line.split(",")));
		    if (ibm.get(ibm.size()-1).equals("false")) {
		    	StringBuilder chain = new StringBuilder();
		    	chain.append(ibm.get(29));
		    	chain.append(ibm.get(30));
		    	chain.append(ibm.get(31));
		    	chain.append(ibm.get(32));
		    	String keychain = chain.toString();
		    	
		    	float valuechain = Float.parseFloat(ibm.get(3));
		    	
		    	output.collect(new Text(keychain),new FloatWritable(valuechain));
		    }
		}//mapper
		
		protected void cleanup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
			//The cleanup job is ran once in each map task, it is most often used for cleaning up resources
		}//cleanup
	}//Map class

                                                                        //Reducer Input Key: Word (Text)
                                                                        //Reducer Input Values: 1 (IntWritable)
                                                                        //Reducer Output Key: Word (Text)
                                                                        //Reducer Output Value: Sum of Input Values (IntWritable)
	public static class Reduce extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable> {
		
		public void configure(JobConf job) {
		}//configure
		protected void setup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
		}//setup
                    //      Word      All values that match key                    Word     Sum of Values
		public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException 
		{
			float sum = 0;
			int i = 0;
            //For each instance of value, add to sum
            while (values.hasNext()) {
            	i++;
            	sum += values.next().get();
            }
            float value = sum/i;
            output.collect(key, new FloatWritable(value));
        	}
        protected void cleanup(OutputCollector<Text, FloatWritable> output) throws IOException, InterruptedException {
		}//cleanup
	}//Reduce Class

    //configurations
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), exercise2.class);
		conf.setJobName("exercise2");

		// conf.setNumReduceTasks(0);

		// conf.setBoolean("mapred.output.compress", true);
		// conf.setBoolean("mapred.compress.map.output", true);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(FloatWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		return 0;
    }//run

    public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new exercise2(), args);
		System.exit(res);
    }//main
}//WordCount
