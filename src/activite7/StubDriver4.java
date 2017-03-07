package activite7;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FilterOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*public class StubDriver {

  public static void main(String[] args) throws Exception {

    /*
     * Validate that two arguments were passed from the command line.
     
    if (args.length != 2) {
      System.out.printf("Usage: StubDriver <input dir> <output dir>\n");
      System.exit(-1);
    }

    
     * Instantiate a Job object for your job's configuration. 
     
    Job job = new Job();
    
    
     * Specify the jar file that contains your driver, mapper, and reducer.
     * Hadoop will transfer this jar file to nodes in your cluster running 
     * mapper and reducer tasks.
     
    job.setJarByClass(StubDriver.class);
    
    /*
     * Specify an easily-decipherable name for the job.
     * This job name will appear in reports and logs.
     
    job.setJobName("Stub Driver");
    job.setMapperClass(StubMapper.class);
    job.setReducerClass(StubReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    /*
     * Start the MapReduce job and wait for it to finish.
     * If it finishes successfully, return 0. If not, return 1.
     
    boolean success = job.waitForCompletion(true);
    System.exit(success ? 0 : 1);
  }
}
*/
public class StubDriver4 extends Configured implements Tool{
	
	  public static void main(String[] args) throws Exception{
		  
		          int exitCode = ToolRunner.run(new StubDriver4(), args);
		  
		          System.exit(exitCode);
		  
		      }
		  
		      public int run(String[] args) throws Exception {
		  
		          if (args.length != 2) {
		  
		              System.err.printf("Usage: %s needs two arguments, input and output files\n", getClass().getSimpleName());
		
		              return -1;
		
		          }
		 
		       
		 
		          Job job = new Job();
		 
		          job.setJarByClass(StubDriver4.class);
		 
		          job.setJobName("DriverClass");
		 
		           
		 
		          FileInputFormat.addInputPath(job, new Path(args[0]));
		 
		          FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 
		       
		 
		          job.setOutputKeyClass(Text.class);
		 		   job.setOutputValueClass(DoubleWritable.class);
		 
		          job.setOutputFormatClass(TextOutputFormat.class);
		           
		 
		           
		 
		          job.setMapperClass(StubMapper.class);
		 
		          job.setReducerClass(StubReducer.class);
		 
		       
		 
		          int returnValue = job.waitForCompletion(true) ? 0:1;
		
		           
		
		          if(job.isSuccessful()) {
		
		              System.out.println("Job was successful");
		  
		          } else if(!job.isSuccessful()) {
		  
		              System.out.println("Job was not successful");          
		  		          }
		  
		           
		  
		          return returnValue;
		  
		      }
		  
		  

}//End DriverClass


