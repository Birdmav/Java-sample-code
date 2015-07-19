package hadoopJoinExample;
import java.io.IOException; 
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.NullWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.io.Writable; 
import org.apache.hadoop.io.WritableComparable; 
import org.apache.hadoop.io.WritableComparator; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat; 
import org.apache.hadoop.util.GenericOptionsParser; 
import org.apache.hadoop.util.ToolRunner;


public class Driver extends org.apache.hadoop.conf.Configured implements org.apache.hadoop.util.Tool{
	public static class JoinGroupingComparator extends WritableComparator {
	    public JoinGroupingComparator() {
	        super (StudenIdKey.class, true);
	    }                             

	    @Override
	    public int compare (WritableComparable a, WritableComparable b){
	        StudenIdKey first = (StudenIdKey) a;
	        StudenIdKey second = (StudenIdKey) b;
	                      
	        return first.studentId.compareTo(second.studentId);
	    }
	}
	public static class JoinSortingComparator extends WritableComparator {
	    public JoinSortingComparator()
	    {
	        super (StudenIdKey.class, true);
	    }
	                               
	    @Override
	    public int compare (WritableComparable a, WritableComparable b){
	        StudenIdKey first = (StudenIdKey) a;
	        StudenIdKey second = (StudenIdKey) b;
	                                 
	        return first.compareTo(second);
	    }
	}
	
	public static class UsschoolMapper extends Mapper<LongWritable, Text, StudenIdKey, JoinGenericWritable>{
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {                           
	        String[] recordFields = value.toString().split(",");
	        String country = recordFields[0];
	        int stateid = Integer.parseInt(recordFields[1]);
	        String districtid = recordFields[2];
	        String schoolid = recordFields[3];
	        String sectionid =recordFields[4];
	        int studentid = Integer.parseInt(recordFields[5]);

	                                               
	        StudenIdKey recordKey = new StudenIdKey(studentid, StudenIdKey.USSCHOOL_RECORD);
	        UsschoolRecord record = new UsschoolRecord( country,  stateid, districtid, 
					 schoolid, sectionid );
	                                               
	        JoinGenericWritable genericRecord = new JoinGenericWritable(record);
	        context.write(recordKey, genericRecord);
	    }
	}
	               
	public static class StudentMapper extends Mapper<LongWritable, Text, StudenIdKey, JoinGenericWritable>{
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String[] recordFields = value.toString().split(",");
	        String id =  recordFields[0];
	        int itemid = Integer.parseInt(recordFields[1]);
	        int score = Integer.parseInt(recordFields[2]);
	        String term = recordFields[3];
	        int studentId = Integer.parseInt(recordFields[4]);
	        int coreid = Integer.parseInt(recordFields[5]);
	                                               
	        StudenIdKey recordKey = new StudenIdKey(studentId, StudenIdKey.STUDENT_RECORD);
	        StudentRecord record = new StudentRecord(id,itemid, score,term, coreid);
	        JoinGenericWritable genericRecord = new JoinGenericWritable(record);
	        context.write(recordKey, genericRecord);
	    }
	}
	
	public static class JoinRecuder extends Reducer<StudenIdKey, JoinGenericWritable, NullWritable, Text>{
	    public void reduce(StudenIdKey key, Iterable<JoinGenericWritable> values, Context context) throws IOException, InterruptedException{
	        StringBuilder output2 = new StringBuilder();
	        int count = 0;
	        List<String> mylist= new ArrayList<String>();

	        //StringBuilder output = new StringBuilder();	 
	        for (JoinGenericWritable v : values) {
	            Writable record = v.get();
	            if (key.recordType.equals( StudenIdKey.USSCHOOL_RECORD)){
	            	UsschoolRecord record2 = (UsschoolRecord)record;
	                output2.append(record2.country).append(",");
	                output2.append(record2.stateid.toString()).append(",");
	                output2.append(record2.districtid).append(",");
	                output2.append(record2.schoolid).append(",");
	                output2.append(record2.sectionid);
	                //output2.append(Integer.parseInt(key.studentId.toString()));     
	            } else {
	    	        StringBuilder output = new StringBuilder();	
	                StudentRecord pRecord = (StudentRecord)record;
	                output.append(Integer.parseInt(key.studentId.toString())).append(",");
	                output.append(pRecord.id).append(",");
	                output.append(pRecord.itemid.toString()).append(",");
	                output.append(pRecord.score).append(",");
	                output.append(pRecord.term).append(",");
	                output.append(pRecord.coreid.toString());
	                mylist.add(output.toString());
	            }
	        }
	        	for(int i=0;i<mylist.size();i++)
	 		       context.write(NullWritable.get(), new Text(mylist.get(i)+"," + output2.toString()));
	    }
	}
	
	public int run(String[] allArgs) throws Exception {
	    String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
	                               
	    Job job = Job.getInstance(getConf());
	    job.setJarByClass(Driver.class);
	                               
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	                               
	    job.setMapOutputKeyClass(StudenIdKey.class);
	    job.setMapOutputValueClass(JoinGenericWritable.class);
	                               
	    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, StudentMapper.class);
	    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, UsschoolMapper.class);
	                              
	    job.setReducerClass(JoinRecuder.class);
	                         
	    job.setSortComparatorClass(JoinSortingComparator.class);
	    job.setGroupingComparatorClass(JoinGroupingComparator.class);
	                               
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	                               
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    boolean status = job.waitForCompletion(true);
	    if (status) {
	        return 0;
	    } else {
	        return 1;
	    }             
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    int res = ToolRunner.run(new Driver(), args);
	}
}
