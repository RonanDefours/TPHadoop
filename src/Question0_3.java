
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class Question0_3 {
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
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
					context.write(new Text(country.toString()), new Text(tag));
				}
			}
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		HashMap<String,Integer> hashMap;
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			hashMap = new HashMap<String, Integer>();
			Comparator<StringAndInt> comparator = new Comparator<StringAndInt>() {
				@Override
				public int compare(StringAndInt o1, StringAndInt o2) {
					return o1.compareTo(o2);
				}
			};
			MinMaxPriorityQueue<StringAndInt> minMax = MinMaxPriorityQueue.orderedBy(comparator).maximumSize(13).create();
			
			for (Text value : values) {
				String stringValue = value.toString();
				if(!hashMap.containsKey(stringValue)) {
					hashMap.put(stringValue, 1);
				}else {
					hashMap.replace(stringValue, hashMap.get(stringValue)+1);
				}
			}
			for(String stringLoop : hashMap.keySet()) {
				minMax.add(new StringAndInt(stringLoop,hashMap.get(stringLoop)));
			}
			
			//Quand on set le max size via un argument, on va juste devoir afficher toute la liste 
			context.write(key,new Text(minMax.peekLast().tag+minMax.peekLast().occurences));
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		String k = otherArgs[2];
		//TODO

		Job job = Job.getInstance(conf, "Question0_0");
		job.setJarByClass(Question0_3.class);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

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