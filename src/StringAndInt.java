import org.apache.hadoop.io.IntWritable;

public class StringAndInt implements Comparable<StringAndInt> {

	String tag;
	IntWritable occurences;

	public StringAndInt(String tag, Integer occurences) {
		super();
		this.tag = tag;
		this.occurences = new IntWritable(occurences.intValue());
	}

	@Override
	public int compareTo(StringAndInt arg0) {
		return this.occurences.compareTo(arg0.occurences);
	}
}
