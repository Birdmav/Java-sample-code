package mibDiff;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;


/*
This was a java script I wrote to check for difference of machine configuration files.  It helped me to automate part of my
work flow
*/

public class mibDiff {
	
	
 
	
 public static ArrayList<String> OpenMib(String path) throws IOException
	  {
		 List<String> lines = Files.readAllLines(Paths.get(path), StandardCharsets.UTF_8);
		 ArrayList<String> needs = new ArrayList<String>();
		 String mibName=null;
		 
		 for(String s:lines)
		  {
		   CharSequence check = "mdUVtx1sJ2K4K";
		   CharSequence checka = "mdUVtx4s";
		   CharSequence check2 = ")";
		   CharSequence check2a = "INDEX";
		   CharSequence check2b= "Please";
		   CharSequence check2c = "*";
		   CharSequence check3 = "OBJEC";
		   CharSequence check4 = "::=";
		   CharSequence check5 = "--";
		   CharSequence check6 = "\"";
		  
		   if((s.contains(check) || s.contains(checka)) && (!s.contains(check2) && !s.contains(check3) && !s.contains(check2a) && !s.contains(check2b) && !s.contains(check2c)))
			 {
		      StringTokenizer st = new StringTokenizer(s);   
			  mibName = st.nextToken();
			  if(!mibName.contains(check4) && !mibName.contains(check5) && !mibName.contains(check6))
			   if(!needs.contains(mibName))
				  needs.add(mibName);
		     }
		  }
		   
		  // Collections.sort(needs);
		 System.out.println(needs.size());
		   return needs;
	   }
 
 public static ArrayList<String> mibTables(Collection<String> list)
  {
	 String output="";
	 ArrayList<String> mibList = new ArrayList<String>();
	 
	 for(String s:list)
	  {
	    output="{ \""+s+"\" },";
	    mibList.add(output);
	  }
	 return mibList;
  }
 
 public static void exportToTxt(Collection<String> list) throws IOException
  {
	 try 
	  {
	    PrintWriter out = new PrintWriter("C:/Users/PBanerjee/Documents/UVTX4K/MibList.txt");
	    for(String s: list)
	     {
	       out.println(s);	
	     }
	    
	    out.close();
	  }
	 catch(IOException e)
	 {
		 e.printStackTrace();
	 }
  }
	
 public static ArrayList<String> OpenCpp(String path) throws IOException
  {
     int begin=0,end=0;
	 List<String> lines = Files.readAllLines(Paths.get(path), StandardCharsets.UTF_8);
	 ArrayList<String> needs = new ArrayList<String>();
	 String trim;
		
	 for(String s:lines)
	  {
	   CharSequence check = "md";
	   if(s.contains(check))
		{ 
	      int count = 0;
		  String q = "\"";
		  char qq = q.charAt(0);
			 
	      for(int i = 0; i < s.length(); i++)
	       {
		    if(s.charAt(i)==qq)
		     {
			  if(count==0)
			   begin=i+1;		  
			  if(count==1)
			   end=i;
			  count++;
		     } 
	        }  
	        trim = s.substring(begin,end);
	        if(!needs.contains(trim))
				  needs.add(trim);
	      }		   
        }	
	   //System.out.println(needs);
	   //HashSet<String> cppMibMap = new HashSet<String>(needs);
	  // for (String line : needs) {
	    //  cppMibMap.add(line);
	  // }
	   Collections.sort(needs);
	   System.out.println(needs.size());
	   return needs;
   }

 public static void main(String[] args) {
		// TODO Auto-generated method stub
	 Collection<String> kMibList = new ArrayList<String>();
	 Collection<String> regMibList = new ArrayList<String>();
	 Collection<String> cppList = new ArrayList<String>();
      try
      {
    	  System.out.println("J2k4k Mib");
    	  kMibList = OpenMib("C:/Users/PBanerjee/Desktop/Email/MD8000-PRIVATE-UVTX-1Sch-J2K4K-MIB");	
      }
     catch(Exception e)
      {
   	  e.printStackTrace();
      }
      try
      {
    	 System.out.println("J2k Mib");
    	 
		 regMibList = OpenMib("C:/Users/PBanerjee/Desktop/Email/MD8000-PRIVATE-UVTX-4Sch-MIB");	
      }
     catch(Exception e)
      {
   	  e.printStackTrace();
      } 
      try
      {
    	
    	cppList = OpenCpp("C:/Users/PBanerjee/Desktop/Email/CPP.txt");
      }
     catch(Exception e)
      {
   	  e.printStackTrace();
      }
      
      ArrayList<String> k = new ArrayList<String>();
      
      k = mibTables(kMibList);
      
      
      try
       {
        exportToTxt(k);
       }
     catch(Exception e)
       {
   	     e.printStackTrace();
       }
     	
     // regMibList.removeAll(cppList);
      System.out.println("");
      System.out.println(k);
      System.out.println("4k:"+kMibList.size());
      System.out.println("");
     // System.out.println(regMibList.size());
      //System.out.println(regMibList);
     // System.out.println(cppList);
      System.out.println("");
 }

}
