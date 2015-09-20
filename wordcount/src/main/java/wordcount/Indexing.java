package wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//http://www.dcs.gla.ac.uk/~richardm/papers/IPM_MapReduce.pdf

public class Indexing {

	private static String kDoc = "doc-id";
	private static String kTf = "tf";

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, MapWritable> {
		// private Text file = new Text();
		private String fileName;
		private static int words = 0;

		@Override
		protected void setup(
				Mapper<Object, Text, Text, MapWritable>.Context context)
				throws IOException, InterruptedException {
			fileName = ((FileSplit) context.getInputSplit()).getPath()
					.toString();
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				words++;
				String word = itr.nextToken().toLowerCase();
				MapWritable map = new MapWritable();
				map.put(new Text(kDoc), new Text(fileName));
				map.put(new Text(kTf), new IntWritable(1));
				context.write(new Text(word), map);
			}
		}
	}

	public static class SearchReducer extends
			Reducer<Text, MapWritable, Text, ArrayWritable> {

		private Map<String, Integer> invertedIndexes = new HashMap<String, Integer>();

		public void reduce(Text key, Iterable<MapWritable> values,
				Context context) throws IOException, InterruptedException {

			ArrayWritable array = new ArrayWritable(Text.class);
			for (MapWritable val : values) {
				String docName = val.get(new Text(kDoc)).toString();

				if (invertedIndexes.get(docName) != null) {
					Integer count = invertedIndexes.get(docName);
					invertedIndexes.put(docName, count.intValue() + 1);
				} else {
					invertedIndexes.put(docName, 0);
				}
			}

			List<Text> indexes = new ArrayList<Text>();

			for (String iterable_element : invertedIndexes.keySet()) {
				indexes.add(new Text(iterable_element + " "
						+ invertedIndexes.get(iterable_element)));
			}

			Text[] out = new Text[indexes.size()];
			array.set(out);
			context.write(new Text(key), array);
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
		job.setOutputValueClass(MapWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}