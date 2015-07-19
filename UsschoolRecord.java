package hadoopJoinExample;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;


public class UsschoolRecord implements Writable {
	public Text country = new Text();
	public IntWritable stateid = new IntWritable();
	public Text districtid = new Text();
	public Text schoolid = new Text();
	public Text sectionid = new Text();

    public UsschoolRecord(){}              

    public UsschoolRecord(String country, int stateid, String districtid, 
    						String schoolid, String sectionid) {
    	this.country.set(country);
    	this.districtid.set(districtid);
    	this.schoolid.set(schoolid);
    	this.sectionid.set(sectionid);
    	this.stateid.set(stateid);
}

    public void write(DataOutput out) throws IOException {
        this.country.write(out);
        this.districtid.write(out);
        this.sectionid.write(out);
        this.stateid.write(out);
        this.schoolid.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.country.readFields(in);
        this.districtid.readFields(in);
        this.sectionid.readFields(in);
        this.stateid.readFields(in);
        this.schoolid.readFields(in);

    }
}

