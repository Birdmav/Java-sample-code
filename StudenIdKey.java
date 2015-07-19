package hadoopJoinExample;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;


public class StudenIdKey implements WritableComparable<StudenIdKey>{
	public IntWritable studentId = new IntWritable();
	public IntWritable recordType = new IntWritable();
	public static final IntWritable STUDENT_RECORD = new IntWritable(0);
	public static final IntWritable USSCHOOL_RECORD = new IntWritable(1);
	public StudenIdKey(){}
	public StudenIdKey(int studentId, IntWritable recordType) {
	    this.studentId.set(studentId);
	    this.recordType = recordType;
	}
	public void write(DataOutput out) throws IOException {
	    this.studentId.write(out);
	    this.recordType.write(out);
	}

	public void readFields(DataInput in) throws IOException {
	    this.studentId.readFields(in);
	    this.recordType.readFields(in); 
	}
	public int compareTo(StudenIdKey other) {
	    if (this.studentId.equals(other.studentId )) {
	        return this.recordType.compareTo(other.recordType);
	    } else {
	        return this.studentId.compareTo(other.studentId);
	    }
	}
	public boolean equals (StudenIdKey other) {
	    return this.studentId.equals(other.studentId) && this.recordType.equals(other.recordType );
	}

	public int hashCode() {
	    return this.studentId.hashCode();
	}
}
