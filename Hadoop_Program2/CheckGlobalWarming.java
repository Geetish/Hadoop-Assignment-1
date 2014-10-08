/* This is the driver class for the mapper and the reducer 
*  Date of Creation : 11/01/2013  
*  Date of Modification : 11/05/2013
*/

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class CheckGlobalWarming {
  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.err.println("Usage: CheckGlobalWarming  ");
      System.exit(-1);
    }
    JobConf conf = new JobConf(CheckGlobalWarming.class);
    conf.setJobName("Max temperature");
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    conf.setMapperClass(CheckGlobalWarmingMapper.class);
    conf.setReducerClass(CheckGlobalWarmingReducer.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
	JobClient.runJob(conf);
  }
}