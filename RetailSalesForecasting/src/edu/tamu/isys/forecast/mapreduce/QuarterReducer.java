package edu.tamu.isys.forecast.mapreduce;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//QuarterReducer will be used to display gross quarter sales
public class QuarterReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
        	
        	double sales = 0;
        	String[] line_values;
        	Map<String, Double> salesYearMap = new TreeMap<String, Double>();
        	StringBuffer monthSales = new StringBuffer();
        	
        	
        	String orderDateMonth = "";
        	String quarter1 = "Quarter 1";
        	String quarter2 = "Quarter 2";
        	String quarter3 = "Quarter 3";
        	String quarter4 = "Quarter 4";
        	for (Text value: values) {
        	//Splitting based on the sign "::" 
        		line_values = value.toString().split(":");
        		sales = Float.parseFloat(line_values[1]);
        		
        		orderDateMonth = line_values[0];
        		//Checking and putting sales values for each quarter-starts
        			if(orderDateMonth.equals("01")||orderDateMonth.equals("02")
        					||orderDateMonth.equals("03"))
            		{
            			if(salesYearMap.containsKey(quarter1)){
                	
            				sales = sales+salesYearMap.get(quarter1);
            				salesYearMap.put((quarter1), sales);
            			}
            			else {
            				salesYearMap.put((quarter1), sales );
            			}
            		}
            	
            		else if(orderDateMonth.equals("04")||orderDateMonth.equals("05")
            				||orderDateMonth.equals("06")){
            			if(salesYearMap.containsKey(quarter2)){
            				sales = sales+salesYearMap.get(quarter2);
            				salesYearMap.put((quarter2), sales);
            			}
            			else {
            		salesYearMap.put((quarter2), sales );
            			}	
            		}
            	
            		else if(orderDateMonth.equals("07")||orderDateMonth.equals("08")
            				||orderDateMonth.equals("09")){
            			if(salesYearMap.containsKey(quarter3)){
            				sales = sales+salesYearMap.get(quarter3);
            				salesYearMap.put((quarter3), sales);
            			}
            			else {
            				salesYearMap.put((quarter3), sales );
            			}	
            		}
            	
            		else if(orderDateMonth.equals("10")||orderDateMonth.equals("11")
            				||orderDateMonth.equals("12")){
            			if(salesYearMap.containsKey(quarter4)){
            				sales = sales+salesYearMap.get(quarter4);
            				salesYearMap.put((quarter4), sales);
            			}
            			else {
            				salesYearMap.put((quarter4), sales );
                	
            			}	
            		}
            	}
        	//Checking and putting sales values for each quarter-ends
        	for (Entry<String, Double> entry : salesYearMap.entrySet()) {
        		monthSales = monthSales.append(System.lineSeparator())
        				.append(entry.getKey()).append(",")
        			    .append(" Total Sales: ")
        			    .append(String.format("%.2f",entry.getValue()))
        			    .append(".");
        	}
        
        	context.write(key, new Text(monthSales.toString()));
 
	}

}