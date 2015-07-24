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




public class Driver extends org.apache.hadoop.conf.Configured implements org.apache.hadoop.util.Tool {
	
	/*
	 * Class and associated methods for joining of data sets on common studentID, compares student IDs from
	 * data sets
	 */
	
	public static class JoinGroupingComparator extends WritableComparator 
	 {
		public JoinGroupingComparator()          //Constructor for Comparator
		 {
		  super(StudentIdKey.class, true);	  //Calling comparator super method
		 }
		
		public int compare(WritableComparable a, WritableComparable b) //Comparing of Writable formats
		 {
			StudentIdKey first = (StudentIdKey) a;     //Casting text fields as StudentIdKeys
			StudentIdKey second = (StudentIdKey) b;
			
			return first.studentId.compareTo(second.studentId);   //Comparing student IDs from both data sets
			
		 }
	 
	 }
	
	/*
	 *   After Joining of data sets, follow inner class for sorting of joined data based on StudentId key
	 */
	
	public static class JoinSortingComparator extends WritableComparator 
	 {
		public JoinSortingComparator()
		 {
			super(StudentIdKey.class, true);     //Calling comparator method in super class
				
	     }
		
		public int compareTo(Writable a, Writable b)       //Sorting based on comparison of StudentIDKeys 
		 {
			StudentIdKey first = (StudentIdKey) a;
			StudentIdKey second = (StudentIdKey) b;
			
			return first.studentId.compareTo(second.studentId);  //return comparison of two values
		 }
		
		
      }
	/*
	 * Inner class for mapping of Us School data set using Primitive values such as LongWritable Text 
	 */
	
	public static class USSchoolMapper extends Mapper<LongWritable, Text, StudentIdKey, JoinGenericWritable>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		 {
		   String[] recordsFields = value.toString().split(",");    //Splitting of each line based on delimiter of ,
		   String country = recordsFields[0];                   //Storing relevant records from each data set line into variables
		   int stateid = Integer.parseInt(recordsFields[1]);    //Some are numbers that need to be parsed
		   String districtid = recordsFields[2];
		   String schoolid = recordsFields[3];
		   String sectionid = recordsFields[4];
		   int studentid = Integer.parseInt(recordsFields[5]);
		   
		   StudentIdKey recordKey = new StudentIdKey(studentid, StudentIdKey.USSCHOOL_RECORD); //Creating key for which map function
		   USSchoolRecord record = new USSchoolRecord(country, stateid, districtid, schoolid, sectionid); //Creates record object for rest of variables
		   
		   JoinGenericWritable genericRecord = new JoinGenericWritable(record); //Creating generic record object from recourd object
		   context.write(recordKey,  genericRecord);  //Writing to context (output) with key value pair, key --> StudentIdKey, value--> generic record object
		 } 
	}
	
	/*
	 * Inner class for mapping of Core Record data set using Primitive values such as LongWritable Text 
	 */
   
	public static class CoreMapper extends Mapper<LongWritable, Text, StudentIdKey, JoinGenericWritable>
	 {
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException //map function for mapping of core records to studentIdKey
		 {
		   String[] recordFields = value.toString().split(",");     //Splitting input line using delimiter and storing in array of Strings
		   String id = recordFields[0];          //Storing each index of String array into instance varables for creation of record object
		   int itemid = Integer.parseInt(recordFields[1]);
		   int score = Integer.parseInt(recordFields[2]);
		   String term = recordFields[3];
		   int studentId = Integer.parseInt(recordFields[4]);  //Student ID which will be turned into key for mapping
		   int coreid = Integer.parseInt(recordFields[5]);
		   
		   StudentIdKey recordKey = new StudentIdKey(studentId, StudentIdKey.CORE_RECORD); //Creating Key with studentID instance variable
		   CoreRecord record = new CoreRecord(id, itemid, score, term,coreid);   //Creaing Core Record object from other instance variables
		  
		   JoinGenericWritable genericRecord = new JoinGenericWritable(record);   //Creating of generic record object from core record object
		   context.write(recordKey,  genericRecord);  //Writing to output the key and the generic record, Key/Value pair
		   
		 }
	 }
 /*
  * JoinReducer class to Aggregate all the values to unique keys and then Join the Datasets based on the keys
  */
  public static class JoinReducer extends Reducer<StudentIdKey, JoinGenericWritable, NullWritable, Text>
   {
	 //reduce method to aggregate all the values to unique StudentIdKeys
	 public void reduce(StudentIdKey key, Iterable<JoinGenericWritable> values, Context context) throws IOException, InterruptedException
	   {
		  StringBuilder output = new StringBuilder();    //String Builder constructor to create output   
		  
		  List<String> list = new ArrayList<String>();   //Creating ArrayList of Strings 
		  
		  for(JoinGenericWritable v: values)     //For every value in record, aggregate values together 
		   {
			 Writable record = v.get();     //Get record
			 
			 if(key.recordType.equals(StudentIdKey.USSCHOOL_RECORD))      
			  {
				 USSchoolRecord urecord = (USSchoolRecord)record;      //Appending and aggregating values together for specific StudentIdKey
				 output.append(urecord.country).append(",");    //Appending fields of records
				 output.append(urecord.stateid.toString()).append(",");
				 output.append(urecord.districtid).append(",");
				 output.append(urecord.schoolid).append(",");
				 output.append(urecord.sectionid);
				 		
			  }
			 else
			  {
				 StringBuilder noutput = new StringBuilder();   //doing same thing for Core Records data set
				 CoreRecord crecord = (CoreRecord)record;   //Creating core record object
				 noutput.append(Integer.parseInt(key.studentId.toString())).append(",");  //Aggregating values together for specific key
				 noutput.append(crecord.id).append(",");
				 noutput.append(crecord.itemid.toString()).append(",");
				 noutput.append(crecord.score).append(",");
				 noutput.append(crecord.term).append(",");
				 noutput.append(crecord.coreid.toString());
				 list.add(output.toString());     //Adding final output to array value of Strings
			  }
		   }
		  
		  for(int i=0;i<list.size();i++)       //Joining values together by iterating through lists and combining on unique keys and delimiting by ,
		       context.write(NullWritable.get(), new Text(list.get(i)+"," + output.toString()));
	   }
   }
		
  
  /*
   Creating Map/Reduce jobs using the innter classes from above
  */
  public int run(String[] allArgs) throws Exception {
	    String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
	                               
	    Job job = Job.getInstance(getConf());
	    job.setJarByClass(Driver.class);
	                               
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	                               
	    job.setMapOutputKeyClass(StudentIdKey.class);
	    job.setMapOutputValueClass(JoinGenericWritable.class);
	                               
	    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CoreMapper.class);
	    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, USSchoolMapper.class);
	                              
	    job.setReducerClass(JoinReducer.class);
	                         
	    job.setSortComparatorClass(JoinSortingComparator.class);
	    job.setGroupingComparatorClass(JoinGroupingComparator.class);
	                               
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	      
	     /*
	     Settting output format and file for joined data sets
	     */                         
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    boolean status = job.waitForCompletion(true);
	    if (status) {
	        return 0;
	    } else {
	        return 1;
	    }             
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
	    int res = ToolRunner.run(new Driver(), args);
	}

}
