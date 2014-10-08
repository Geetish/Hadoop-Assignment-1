/* 
* This class is used as a reducer to calcuate the average monthly temperature, standard deviation and 95% confidence interval
* Date of Creation : 1st Nov 2013                  Date of Last Modification : 4th Nov 2013
*/

import java.io.IOException; 
import java.util.Iterator;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer; import org.apache.hadoop.mapred.Reporter;
	
public class AverageMonthlyReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {

	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException{ 
                        double sum = 0;
			   int count = 0;
			   ArrayList<Integer> temperatureValues = new ArrayList<Integer>();
                        while (values.hasNext()) {
			 		int nextValue = values.next().get();
					nextValue = nextValue/10;
					sum += nextValue;
					count++;
					temperatureValues.add(nextValue);
			   }
			   double mean = (count!=0) ? (sum/count) : 0;
			   double stdDev = standardDeviation(temperatureValues,mean,count);
			   String confidenceInterval = get95ConfidenceInterval(mean,count, stdDev);
			   Text outputText = new Text("[ Mean:" + mean+"    Std Dev:"+stdDev+" " + "    Confidence Interval: Lower:- " + confidenceInterval.split("_")[0] + "      Upper :-"+ confidenceInterval.split("_")[1] + "]"); 
			   output.collect(key, outputText);			
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
	 
	 /* Function to get the confidence interval
	 * @param mean The mean temperature for a specific year
	 * @param n The number of temperature values
	 * @param stdDev
	 */
	 private String get95ConfidenceInterval(double mean, int n, double stdDev){
		double lowerRange = (n!=0) ? mean - (1.96 * (stdDev/Math.pow(n,0.5))) : 0;
		double upperRange = (n!=0) ? mean + (1.96 * (stdDev/Math.pow(n,0.5))) : 0;
		return ""+lowerRange+"_"+upperRange;
	 }
}