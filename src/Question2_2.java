
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.collect.MinMaxPriorityQueue;

public class Question2_2 {
	public static class MyMapper extends Mapper<LongWritable, Text, Text, StringAndInt> {
		@SuppressWarnings("deprecation")
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("\t");
			// String decodedLine = java.net.URLDecoder.decode(line);
			System.out.println(line[10] + " : " + line[11]);
			double longitude = Double.parseDouble(line[10]);
			double latitude = Double.parseDouble(line[11]);
			Country country = Country.getCountryAt(latitude, longitude);
			if (country != null) {
				for (String tag : line[8].split(",")) {
					context.write(new Text(country.toString()), new StringAndInt(tag, (int)1));
				}
			}
		}
	}

	public static class MyReducer extends Reducer<Text, StringAndInt, Text, Text> {
		HashMap<String, Integer> hashMap;

		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values, Context context)
				throws IOException, InterruptedException {

			int numOfTags = context.getConfiguration().getInt("numberOfTags", -1);

			hashMap = new HashMap<String, Integer>();
			Comparator<StringAndInt> comparator = new Comparator<StringAndInt>() {
				@Override
				public int compare(StringAndInt o1, StringAndInt o2) {
					return o2.compareTo(o1);
				}
			};
			MinMaxPriorityQueue<StringAndInt> minMax = MinMaxPriorityQueue.orderedBy(comparator).maximumSize(numOfTags)
					.create();

			for (StringAndInt value : values) {
				StringAndInt stringAndInt = value;
				if (!hashMap.containsKey(stringAndInt.getTag())) {
					hashMap.put(stringAndInt.getTag(), stringAndInt.getOccurences().get());
				} else {
					hashMap.replace(stringAndInt.getTag(), hashMap.get(stringAndInt.getTag()) + stringAndInt.getOccurences().get());
				}
			}
			for (String stringLoop : hashMap.keySet()) {
				minMax.add(new StringAndInt(stringLoop, hashMap.get(stringLoop)));
			}
			// We print the whole queue for a given country, since the nuber of tags we want
			// has been set through the command line.
			while (!minMax.isEmpty()) {
				StringAndInt element = minMax.removeFirst();
				// Check that the element exists and that the tag is at least a little
				// meaningful.
				if (!(element == null) && !(element.getTag() == null) && !element.getTag().equals("")) {
					context.write(key, new Text(element.getTag() + " " + element.getOccurences()));
				}
			}
		}
	}

	private static class MyCombiner extends Reducer<Text, StringAndInt, Text, StringAndInt> {
		HashMap<String, Integer> hashMap;

		@Override
		protected void reduce(Text key, Iterable<StringAndInt> values, Context context)
				throws IOException, InterruptedException {

			hashMap = new HashMap<String, Integer>();

			for (StringAndInt value : values) {
				String stringValue = value.getTag();
				if (!hashMap.containsKey(stringValue)) {
					hashMap.put(stringValue, value.getOccurences().get());
				} else {
					hashMap.replace(stringValue, hashMap.get(stringValue) + value.getOccurences().get());
				}
			}

			for (Entry<String, Integer> entry : hashMap.entrySet()) {
				StringAndInt encapsulatedStringAndInt = new StringAndInt(entry.getKey(), entry.getValue().intValue());
				context.write(key, encapsulatedStringAndInt);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		String numberOfTags = otherArgs[2];
		conf.setInt("numberOfTags", Integer.parseInt(numberOfTags));

		Job job = Job.getInstance(conf, "Question0_3");
		job.setJarByClass(Question2_2.class);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StringAndInt.class);

		job.setCombinerClass(MyCombiner.class);
		
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}