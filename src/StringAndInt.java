import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class StringAndInt implements Comparable<StringAndInt>, Writable {

	private String tag;
	private IntWritable occurences;

	public StringAndInt() {
		super();
		setOccurences(new IntWritable());
	}
	
	public StringAndInt(String tag, Integer occurences) {
		super();
		this.setTag(tag);
		this.setOccurences(new IntWritable(occurences.intValue()));
	}

	@Override
	public int compareTo(StringAndInt arg0) {
		return this.getOccurences().compareTo(arg0.getOccurences());
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		int occ = arg0.readInt();
		getOccurences().set(occ);
		setTag(arg0.readUTF());
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(getOccurences().get());
		arg0.writeUTF(getTag());
	}
	
	public String toString() {
		return getTag()+","+getOccurences();
	}
	
	public void fromString(String string) {
		String[] split = string.split(",");
		setTag(split[0]);
		setOccurences(new IntWritable(Integer.parseInt(split[1])));
	}

	IntWritable getOccurences() {
		return occurences;
	}

	void setOccurences(IntWritable occurences) {
		this.occurences = occurences;
	}

	String getTag() {
		return tag;
	}

	void setTag(String tag) {
		this.tag = tag;
	}
}
