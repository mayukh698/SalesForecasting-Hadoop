package edu.tamu.isys.forecast.mapreduce;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//Class will be used for gross monthly sales (without product category or province)
public class MonthReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
        	
        	double sales = 0;
        	String[] line_values;
        	Map<String, Double> salesYearMap = new TreeMap<String, Double>();
        	//monthMap will be used to display the month in result
        	Map<String, String> monthMap = new TreeMap<String, String>();
        	monthMap.put("01", "Jan");
        	monthMap.put("02", "Feb");
        	monthMap.put("03", "Mar");
        	monthMap.put("04", "Apr");
        	monthMap.put("05", "May");
        	monthMap.put("06", "Jun");
        	monthMap.put("07", "Jul");
        	monthMap.put("08", "Aug");
        	monthMap.put("09", "Sep");
        	monthMap.put("10", "Oct");
        	monthMap.put("11", "Nov");
        	monthMap.put("12", "Dec");
        	StringBuffer monthSales = new StringBuffer();
        	for (Text value: values) {
        	//Splitting based on the sign ":" 
        		line_values = value.toString().split(":");
        		sales = Float.parseFloat(line_values[1]);
        		if(salesYearMap.containsKey(line_values[0])){
        			sales = sales+salesYearMap.get(line_values[0]);
        			salesYearMap.put(line_values[0], sales);
        		}
        		else {
        			sales = new Double(line_values[1]);
        			salesYearMap.put(line_values[0], sales );
            	
        		}
          }
        	for (Entry<String, Double> entry : salesYearMap.entrySet()) {
        		monthSales = monthSales.append(System.lineSeparator())
        				.append("Month:").append(monthMap.get(entry.getKey()))
        				.append(",")
        			    .append(" Total Sales: ")
        			    .append(String.format("%.2f",entry.getValue()))
        			    .append(".");
        	}
        
        	context.write(key, new Text(monthSales.toString()));
 
	}

}