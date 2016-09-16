package edu.tamu.isys.forecast.mapreduce;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//QuarterGrossReducer will be used for gross sales for each quarter
public class QuarterGrossReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
        	String[] line_values;
        	String output = "";
        	String orderDate = "";
        	String[] monthValue;
        	String orderDateYear = "";
        	String orderDateMonth = "";
        	String quarter1 = "Quarter 1";
        	String quarter2 = "Quarter 2";
        	String quarter3 = "Quarter 3";
        	String quarter4 = "Quarter 4";
        	double sales = 0;
        	StringBuffer quarterSales = new StringBuffer();
        	Map<String, Double> salesYearMap = new TreeMap<String, Double>();//created to store movie name and rating
        	//Looping through the values from Mapper
        	for (Text value: values) {
        	//Splitting based on the sign "::" 
        		line_values = value.toString().split("::");
            
        		sales = Float.parseFloat(line_values[1]);
            
        		orderDate = line_values[0].toString();
        		monthValue = orderDate.split("-");
        		orderDateYear = monthValue[2];
        		orderDateMonth = monthValue[1];
        	
        		//Checking and putting sales values for each quarter-starts
        		if(orderDateMonth.equals("01")||orderDateMonth.equals("02")
        				||orderDateMonth.equals("03"))
        		{
        			if(salesYearMap.containsKey(quarter1+","+orderDateYear)){
            	
        				sales = sales
        						+salesYearMap.get(quarter1+","+orderDateYear);
        				salesYearMap.put((quarter1+","+orderDateYear), sales);
        			}
        			else {
        				salesYearMap.put((quarter1+","+orderDateYear), sales );
        			}
        		}
        	
        		else if(orderDateMonth.equals("04")||orderDateMonth.equals("05")
        				||orderDateMonth.equals("06")){
        			if(salesYearMap.containsKey(quarter2+","+orderDateYear)){
        				sales = sales
        						+salesYearMap.get(quarter2+","+orderDateYear);
        				salesYearMap.put((quarter2+","+orderDateYear), sales);
        			}
        			else {
        		salesYearMap.put((quarter2+","+orderDateYear), sales );
        			}	
        		}
        	
        		else if(orderDateMonth.equals("07")||orderDateMonth.equals("08")
        				||orderDateMonth.equals("09")){
        			if(salesYearMap.containsKey(quarter3+","+orderDateYear)){
        				sales = sales
        						+salesYearMap.get(quarter3+","+orderDateYear);
        				salesYearMap.put((quarter3+","+orderDateYear), sales);
        			}
        			else {
        				salesYearMap.put((quarter3+","+orderDateYear), sales );
        			}	
        		}
        	
        		else if(orderDateMonth.equals("10")||orderDateMonth.equals("11")
        				||orderDateMonth.equals("12")){
        			if(salesYearMap.containsKey(quarter4+","+orderDateYear)){
        				sales = sales
        						+salesYearMap.get(quarter4+","+orderDateYear);
        				salesYearMap.put((quarter4+","+orderDateYear), sales);
        			}
        			else {
        				salesYearMap.put((quarter4+","+orderDateYear), sales );
            	
        			}	
        		}
        	}
        	//Checking and putting sales values for each quarter-ends
        for (Entry<String, Double> entry : salesYearMap.entrySet()) {
        	quarterSales = quarterSales.append(System.lineSeparator())
        			.append(entry.getKey()).append(",")
        			.append(" Total Sales: ")
        			.append(String.format("%.2f",entry.getValue()))
        			.append(".");
        			
            

        }
        output = quarterSales.toString();
        quarterSales.delete(0, quarterSales.length());
        
        context.write(key, new Text(output));
        
	}

}