/* The purpose of this Mapper class is to generate <Key,Value> pairs which is used by the Reducer
*  The Key is in the form of Year_Month
*  The Value is the corresponding temperature 
*  Date of Creation : 11/01/2013  
*  Date of Modification : 11/05/2013
*/

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CheckGlobalWarmingMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

  private static final int MISSING = 9999;
  public void map(LongWritable key, Text value,
      OutputCollector output, Reporter reporter)
      throws IOException {
    String line = value.toString();
	// Get the year first
    String year = line.substring(15, 19);
	// Get the month
	String month = line.substring(19,21);
    int airTemperature;
    if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
      airTemperature = Integer.parseInt(line.substring(88, 92));
    } else {
      airTemperature = Integer.parseInt(line.substring(87, 92));
    }
    String quality = line.substring(92, 93);
    if (airTemperature != MISSING && quality.matches("[01459]")) {
	  airTemperature = airTemperature/10;
	  // Key is in the format year_month for example "1901_01"
      output.collect(new Text(year+"_"+month), new IntWritable(airTemperature));
	  output.collect(new Text(year), new IntWritable(airTemperature));
    }
  }
}