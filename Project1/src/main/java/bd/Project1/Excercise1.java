package bd.Project1;

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
import org.apache.hadoop.util.GenericOptionsParser;

public class Excercise1 {
	public static void main(String[] args) throws Exception {
		Configuration c = new Configuration();
		String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		Job j = new Job(c, "wordcount");
		j.setJarByClass(Excercise1.class);
		j.setMapperClass(Map.class);
		j.setReducerClass(Reduce.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true) ? 0 : 1);
	}

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
			String[] words = value.toString().split(",");
			for (String word : words) {
				if (word.toUpperCase().contains("TRUMP")) {
					Text outputKey = new Text("TRUMP");
					IntWritable outputValue = new IntWritable(1);
					con.write(outputKey, outputValue);
				}
				else if (word.toUpperCase().contains("FLU")) {
					Text outputKey = new Text("FLU");
					IntWritable outputValue = new IntWritable(1);
					con.write(outputKey, outputValue);
				}
				else if (word.toUpperCase().contains("ZIKA")) {
					Text outputKey = new Text("ZIKA");
					IntWritable outputValue = new IntWritable(1);
					con.write(outputKey, outputValue);
				}else if (word.toUpperCase().contains("DIARRHEA")) {
					Text outputKey = new Text("DIARRHEA");
					IntWritable outputValue = new IntWritable(1);
					con.write(outputKey, outputValue);
				}else if (word.toUpperCase().contains("EBOLA")) {
					Text outputKey = new Text("EBOLA");
					IntWritable outputValue = new IntWritable(1);
					con.write(outputKey, outputValue);
				}else if (word.toUpperCase().contains("HEADACHE")) {
					Text outputKey = new Text("HEADACHE");
					IntWritable outputValue = new IntWritable(1);
					con.write(outputKey, outputValue);
				}else if (word.toUpperCase().contains("MEASLES")) {
					Text outputKey = new Text("MEASLES");
					IntWritable outputValue = new IntWritable(1);
					con.write(outputKey, outputValue);
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text word, Iterable<IntWritable> values, Context con)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			con.write(word, new IntWritable(sum));
		}
	}
}