package hadoopJoinExample;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class StudentRecord implements Writable {
	public Text id = new Text();
	public IntWritable itemid = new IntWritable();
	public IntWritable score = new IntWritable();
	public Text term = new Text();
	public IntWritable coreid = new IntWritable();
	
	
 
    public StudentRecord(){}

    public StudentRecord(String id, int itemid, int score, String term, int coreid){ 
    	this.id.set(id);
    	this.itemid.set(itemid);
    	this.score.set(score);
    	this.term.set(term);
    	this.coreid.set(coreid);
    }
    
          
    public void write(DataOutput out) throws IOException {
        this.id.write(out);
        this.itemid.write(out);
        this.coreid.write(out);
        this.term.write(out);
        this.score.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.id.readFields(in);
        this.itemid.readFields(in);
        this.coreid.readFields(in);
        this.term.readFields(in);
        this.score.readFields(in);
    }
}

