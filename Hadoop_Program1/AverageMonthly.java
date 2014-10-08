/* 
* This is the driver function for the mapper and the reducer
* Date of Creation : 1st Nov 2013                  Date of Last Modification : 4th Nov 2013
*/
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class AverageMonthly {
  public static void main(String[] args) throws IOException {
    if (args.length != 2) {
      System.err.println("Usage: AverageMonthly  ");
      System.exit(-1);
    }
    JobConf conf = new JobConf(AverageMonthly.class);
    conf.setJobName("Average Monthly Temperature");

    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    conf.setMapperClass(AverageMonthlyMapper.class);
    conf.setReducerClass(AverageMonthlyReducer.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
    JobClient.runJob(conf);
  }
}