package wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Indexing {

	private static String SEARCH_KEY = "word";

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, FloatWritable> {

		// private Text file = new Text();
		private String fileName;
		private static int words = 0;

		private List<String> wordIndexes = new ArrayList<String>();

		@Override
		protected void setup(
				Mapper<Object, Text, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {
			fileName = ((FileSplit) context.getInputSplit()).getPath()
					.toString();
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				words++;
				// file.set(fileName + "@" + itr.nextToken());
				// context.write(file, new IntWritable(1));
				wordIndexes.add(fileName + "@" + itr.nextToken());
			}
		}

		@Override
		protected void cleanup(
				Mapper<Object, Text, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {
			for (String entry : wordIndexes) {
				context.write(new Text(entry), new FloatWritable(
						(float) (1.0 / words)));
			}
		}
	}

	public static class SearchReducer extends
			Reducer<Text, FloatWritable, Text, FloatWritable> {

		public void reduce(Text key, Iterable<FloatWritable> values,
				Context context) throws IOException, InterruptedException {
			float tf = 0.0f;
			for (FloatWritable val : values) {
				tf += val.get();
			}

			context.write(key, new FloatWritable(tf));
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Usage : wordcount.Indexing <input> <output>");
			System.exit(0);
		}

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Searching");

		job.setJarByClass(Indexing.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(SearchReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}