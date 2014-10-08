/* The purpose of this Reducer class is to analyse the data that it gets from the mapper and 
*  check if there is an abnormal rise/fall in temperature which is an indicator of global warming
*  Date of Creation : 11/01/2013  
*  Date of Modification : 11/05/2013
*/
import java.io.IOException; 
import java.util.Iterator;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import java.util.Map;
import java.util.HashMap;
import org.apache.hadoop.mapred.Reducer; import org.apache.hadoop.mapred.Reporter;
	
public class CheckGlobalWarmingReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {
	// These maps are used to load the data that we obtain from the previous mapper and the 
	// reducer
	Map<String,Double> mapMonthMean = new HashMap<String,Double>();
	Map<String,Double> mapMonthStdDev = new HashMap<String,Double>();
	CheckGlobalWarmingReducer(){
		// Load data
		loadData();
	}
	/* This function is used to load the data from that we get from the Mapper */
	public void loadData(){
		mapMonthMean.put("Jan",-4.4807);
		mapMonthStdDev.put("Jan",6.0387);
		
		mapMonthMean.put("Feb",-7.5684);
		mapMonthStdDev.put("Feb",6.1818);
		
		mapMonthMean.put("Mar",-5.0232);
		mapMonthStdDev.put("Mar",6.0688);
		
		mapMonthMean.put("Apr",0.3027);
		mapMonthStdDev.put("Apr",4.3925);
		
		mapMonthMean.put("May",6.2858);
		mapMonthStdDev.put("May",5.0372);
		
		mapMonthMean.put("Jun",11.9675);
		mapMonthStdDev.put("Jun",4.9747);
		
		mapMonthMean.put("Jul",15.4560);
		mapMonthStdDev.put("Jul",5.1988);
		
		mapMonthMean.put("Aug",14.4991);
		mapMonthStdDev.put("Aug",3.8843);
		
		
		mapMonthMean.put("Sept",9.6379);
		mapMonthStdDev.put("Sept",3.8107);
		
		mapMonthMean.put("Oct",5.2912);
		mapMonthStdDev.put("Oct",4.4510);
		
		mapMonthMean.put("Nov",-1.2601);
		mapMonthStdDev.put("Nov",5.2827);
		
		mapMonthMean.put("Dec",-6.2518);
		mapMonthStdDev.put("Dec",6.3418);
		
	}
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException{ 
                        // from the list of values, find the maximum
                        double sum = 0;
						int count = 0;
						ArrayList<Integer> temperatureValues = new ArrayList<Integer>();
                        while (values.hasNext()) {
							int nextValue = values.next().get();
							sum += nextValue;
							count++;
							temperatureValues.add(nextValue);
					   }
						double mean = (count!=0) ? (sum/count) : 0;
                        // emit (key = year, value = maxTemp = max for year)
						double stdDev = standardDeviation(temperatureValues,mean,count);
						Text outputText = new Text("[ Mean:" + mean+" Std Dev:"+stdDev+"]" );
						String keyInString = key.toString();
						if(keyInString.indexOf("_")==-1)
							output.collect(key, outputText);
						else{
							double minRange = mapMonthMean.get(getMonth(keyInString.split("_")[1])) - mapMonthStdDev.get(getMonth(keyInString.split("_")[1]));
							double maxRange = mapMonthMean.get(getMonth(keyInString.split("_")[1])) + mapMonthStdDev.get(getMonth(keyInString.split("_")[1]));
							if(mean>minRange && mean<maxRange){
								Text finalOutputText = new Text(outputText.toString() + "    No Alert");
								output.collect(new Text("     "+getMonth(keyInString.split("_")[1])),finalOutputText);
							}
							else
							{
								Text finalOutputText = new Text(outputText.toString() + "    Alert");
								output.collect(new Text("     "+getMonth(keyInString.split("_")[1])),finalOutputText);
							}
						}
	}
	
	/* function for standard deviation
	 * @param temperatureValues An arrayList consisting of temperature values
	 * @param mean The mean of the temperature values
	 * @return The standard deviation
	 */
	 private double standardDeviation(ArrayList<Integer> temperatureValues,double mean, double count) {
		double sumOfSquares= 0;
		for(Integer temperature : temperatureValues){
			sumOfSquares = sumOfSquares + Math.pow((mean - temperature),2);
		}
		double stdDev = (count!=0) ? (sumOfSquares/count) : 0;
		stdDev=Math.pow(stdDev,0.5);
		return stdDev;
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

