package wordcount;

import java.io.IOException;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Searching {

	private static String SEARCH_KEY = "word";
	
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, Text> {

		private Text word = new Text();
		private String localname;
		private static int line = 0;

		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			localname = ((FileSplit) context.getInputSplit()).getPath()
					.toString();
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			line++;
			while (itr.hasMoreTokens()) {
				word.set(localname);
				context.write(word, new Text(line + " : " + itr.nextToken()));
			}
		}
	}

	public static class SearchReducer extends
			Reducer<Text, Text, Text, IntWritable> {

		private static String searchWord = "";
		@Override
		protected void setup(
				Reducer<Text, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			searchWord = context.getConfiguration().get(SEARCH_KEY);
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text val : values) {
				String text = val.toString();
				if (text.contains(searchWord)) {
					String[] parts = text.split(" : ");
					context.write(key, new IntWritable(Integer.parseInt(parts[0])));
				}
			}
		}
	}

	public static Map sortByValue(Map unsortMap) {
		@SuppressWarnings("rawtypes")
		List list = new LinkedList(unsortMap.entrySet());

		Collections.sort(list, new Comparator() {
			public int compare(Object o1, Object o2) {
				return ((Comparable) ((Map.Entry) (o2)).getValue())
						.compareTo(((Map.Entry) (o1)).getValue());
			}
		});

		Map sortedMap = new LinkedHashMap();
		for (Iterator it = list.iterator(); it.hasNext();) {
			Map.Entry entry = (Map.Entry) it.next();
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.out.println("Usage : wordcount.Searching <input> <output> <word_to_search>");
			System.exit(0);
		} else {
			System.out.println(args.toString());
		}
		
		Configuration conf = new Configuration();
		conf.set(SEARCH_KEY, args[2]);	//Setting a variable to pass it to Mapper and Reducer

		Job job = Job.getInstance(conf, "Searching");

		job.setJarByClass(Searching.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(SearchReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}