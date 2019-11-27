
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question3_0 {
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

	public static class MyReducer extends Reducer<Text, StringAndInt, Text, StringAndInt> {
			HashMap<String, Integer> hashMap;

			@Override
			protected void reduce(Text key, Iterable<StringAndInt> values, Context context)
					throws IOException, InterruptedException {

				hashMap = new HashMap<String, Integer>();

			for (StringAndInt value : values) {
				StringAndInt stringAndInt = value;
				if (!hashMap.containsKey(stringAndInt.getTag())) {
					hashMap.put(stringAndInt.getTag(), stringAndInt.getOccurences().get());
				} else {
					hashMap.replace(stringAndInt.getTag(), hashMap.get(stringAndInt.getTag()) + stringAndInt.getOccurences().get());
				}
			}
			for (Entry<String, Integer> entry : hashMap.entrySet()) {

				context.write(new Text(key), new StringAndInt(entry.getKey(),entry.getValue()));
			}
		}
	}

	public static class MyMapper2 extends Mapper<Text, StringAndInt, StringAndInt, Text> {

		@SuppressWarnings("deprecation")
		@Override
		protected void map(Text key, StringAndInt value, Context context) throws IOException, InterruptedException {
			StringAndInt newValue = value;
			newValue.setTag(key +" "+newValue.getTag());
			context.write(newValue, new Text());
		}
	}

	public static class MyReducer2 extends Reducer<IntWritable, Text, Text, IntWritable> {
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int occurences = context.getConfiguration().getInt("numberOfTags", -1);

			for (Text value : values) {
				context.write(value, key);
			}
		}
	}

	public static class GComp extends WritableComparator{
		public GComp(){
		super();
		}

	    @Override
	    public int compare(WritableComparable wc1, WritableComparable wc2) {
	    	
	    	StringAndInt key1 = (StringAndInt)wc1;
	    	StringAndInt key2 = (StringAndInt)wc2;
			return key1.getTag().compareTo(key2.getTag());
	    }
	}
	
	public static class SortComparator extends WritableComparator {
		public SortComparator(){
		super();
		}
		
	    @Override
	    public int compare(WritableComparable wc1, WritableComparable wc2) {
	    	StringAndInt key1 = (StringAndInt)wc1;
	    	StringAndInt key2 = (StringAndInt)wc2;
	    	return key1.getOccurences().compareTo(key2.getOccurences());
	    }
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];
		String numberOfTags = otherArgs[2];
		conf.setInt("numberOfTags", Integer.parseInt(numberOfTags));

		Job job1 = Job.getInstance(conf, "Question0_3");
		job1.setJarByClass(Question3_0.class);

		job1.setMapperClass(MyMapper.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(StringAndInt.class);

		job1.setReducerClass(MyReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(StringAndInt.class);

		FileInputFormat.addInputPath(job1, new Path(input));
		job1.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job1, new Path(output + "/temp"));
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);

		Job job2 = Job.getInstance(conf, "Question0_3");
		job2.setJarByClass(Question2_1.class);

		job2.setMapperClass(MyMapper2.class);
		job2.setMapOutputKeyClass(StringAndInt.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setReducerClass(MyReducer2.class);
		job2.setOutputKeyClass(StringAndInt.class);
		job2.setOutputValueClass(Text.class);

		job2.setGroupingComparatorClass(GComp.class);
		job2.setSortComparatorClass(SortComparator.class);
		
		FileInputFormat.addInputPath(job2, new Path(output + "/temp"));
		job2.setInputFormatClass(SequenceFileInputFormat.class);

		FileOutputFormat.setOutputPath(job2, new Path(output + "/result"));
		job2.setOutputFormatClass(TextOutputFormat.class);

		if (job1.waitForCompletion(true)) {
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		} else {
			System.exit(1);
		}
	}
}