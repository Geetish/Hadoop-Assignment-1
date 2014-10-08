/* 
* This function is the mapper function for the Average Temperature program
* Date of Creation : 1st Nov 2013                  Date of Last Modification : 4th Nov 2013
*/
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class AverageMonthlyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>  {

  private static final int MISSING = 9999;
  public void map(LongWritable key, Text value,
      OutputCollector output, Reporter reporter)
      throws IOException {
	String line = value.toString();
    String month = getMonth(line.substring(19,21).trim());
    int airTemperature;
    if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
      airTemperature = Integer.parseInt(line.substring(88, 92));
    } else {
      airTemperature = Integer.parseInt(line.substring(87, 92));
    }
    String quality = line.substring(92, 93);
    if (airTemperature != MISSING && quality.matches("[01459]")) {
	  output.collect(new Text(month), new IntWritable(airTemperature));
    }
 }
 
 /*
 * This function returns the month equivalent of the number
 * @param month in string
 * @return the string format of the month
 */
 private String getMonth(String monthInNum){
    int choice = Integer.parseInt(monthInNum);
	switch(choice){
		case 1 : return "Jan";
					
		case 2 : return "Feb";
					
		case 3 : return "Mar";
					
		case 4 : return "Apr";
					
		case 5 : return "May";
					
		case 6 : return "Jun";
					
		case 7 : return "Jul";
					
		case 8 : return "Aug";
					
		case 9 : return "Sept";
					
		case 10 : return "Oct";
					
		case 11 : return "Nov";
					
		case 12 : return "Dec";
					
	}
	return "Jan";
 }

}