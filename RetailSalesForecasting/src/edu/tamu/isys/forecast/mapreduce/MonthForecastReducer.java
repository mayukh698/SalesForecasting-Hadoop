package edu.tamu.isys.forecast.mapreduce;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//Class used for monthly forecasting for product category or province
public class MonthForecastReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
        	String[] line_values;
        	String output = "";
        	String orderDate = "";
        	String[] monthValue;
        	
        	String orderDateMonth = "";
        	
        	double sales = 0;
        	StringBuffer monthSales = new StringBuffer();
        	Map<String, Double> salesYearMap = new TreeMap<String, Double>();
        	//monthMap will be used to display the month in result
        	Map<Integer, String> monthMap = new TreeMap<Integer, String>();
        	monthMap.put(1, "Jan");
        	monthMap.put(2, "Feb");
        	monthMap.put(3, "Mar");
        	monthMap.put(4, "Apr");
        	monthMap.put(5, "May");
        	monthMap.put(6, "Jun");
        	monthMap.put(7, "Jul");
        	monthMap.put(8, "Aug");
        	monthMap.put(9, "Sep");
        	monthMap.put(10, "Oct");
        	monthMap.put(11, "Nov");
        	monthMap.put(12, "Dec");
        	for (Text value: values) {
        	//Splitting based on the sign "::" 
        		line_values = value.toString().split("::");
            
        		sales = Float.parseFloat(line_values[1]);
            
        		orderDate = line_values[0].toString();
        		monthValue = orderDate.split("-");
        		
        		orderDateMonth = monthValue[1];
        		//salesYearMap will contain sales value for all months
        		if(salesYearMap.containsKey(orderDateMonth)){
        			sales = sales+salesYearMap.get(orderDateMonth);
        			salesYearMap.put(orderDateMonth, sales);
        		}
        		else {
        			salesYearMap.put(orderDateMonth, sales );
            	
        		}
                       
        	}
        	int month = 1;
        	for (Entry<String, Double> entry : salesYearMap.entrySet()) {
        		
        		
        		String monthlyForecastedSales = 
        				String.format("%.2f",(entry.getValue()/4));
        		monthSales = monthSales.append(System.lineSeparator())
            			.append("Expected Sales for next year, month:")
            			.append(monthMap.get(month)).append(":")
            			.append(monthlyForecastedSales);
        		month++;
        	}
        	output = monthSales.toString();
        	monthSales.delete(0, monthSales.length());
        	context.write(key, new Text(output));
       
     }

}