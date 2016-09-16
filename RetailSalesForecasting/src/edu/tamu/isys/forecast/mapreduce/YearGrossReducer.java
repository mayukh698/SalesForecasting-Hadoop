package edu.tamu.isys.forecast.mapreduce;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//YearGrossReducer used for gross sales for next year
public class YearGrossReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
        	String[] line_values;
        	String output = "";
        	String orderDate = "";
        	String[] yearValue;
        	String orderDateYear = "";
        	double sales = 0;
        	
        	StringBuffer yearSales = new StringBuffer();
        	Map<String, Double> salesYearMap = new TreeMap<String, Double>();
        
           //Looping through the values from Mapper
        	for (Text value: values) {
        	//Splitting based on the sign "::" 
        		line_values = value.toString().split("::");
            
        		sales = Float.parseFloat(line_values[1]);
            
        		orderDate = line_values[0].toString();
        		yearValue = orderDate.split("-");
        		orderDateYear = yearValue[2];
        		//salesYearMap will be used to store sales value for each year
        		if(salesYearMap.containsKey(orderDateYear)){
        			sales = sales+salesYearMap.get(orderDateYear);
        			salesYearMap.put(orderDateYear, sales);
        		}
        		else {
        			salesYearMap.put(orderDateYear, sales );
        		}
        	}
        	//Iterating through the salesYearMap
        	for (Entry<String, Double> entry : salesYearMap.entrySet()) {
        	
        		yearSales = yearSales.append(System.lineSeparator()).append(" Year: ")
        					.append(entry.getKey()).append(",")
        					.append("Total Sales: ")
        					.append(String.format("%.2f",entry.getValue()))
        					.append(".");
        	}
        	output = yearSales.toString();
        	yearSales.delete(0, yearSales.length());
        
        	context.write(key, new Text(output));
 
	}

}