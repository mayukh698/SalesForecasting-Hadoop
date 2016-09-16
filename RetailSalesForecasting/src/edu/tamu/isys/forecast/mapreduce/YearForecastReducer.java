package edu.tamu.isys.forecast.mapreduce;
import java.io.IOException;
import java.util.Map;

import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//YearForecastReducer used for forecasted sales for next year
public class YearForecastReducer extends Reducer<Text, Text, Text, Text> {

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
        		if(salesYearMap.containsKey(orderDateYear)){
        			sales = sales+salesYearMap.get(orderDateYear);
        			salesYearMap.put(orderDateYear, sales);
        		}
        		else {
        			salesYearMap.put(orderDateYear, sales );
        		}
        	}
        	
        	double fSalesNum = salesYearMap.get("2009")
        						+salesYearMap.get("2010")
        						+salesYearMap.get("2011")
        						+salesYearMap.get("2012");
        	double fSalesDenom = 4.0;
        	double fSales = (fSalesNum/fSalesDenom);
        	output = "Expected Sales of year 2013: "
        				+String.format("%.2f",fSales);
        	
        	yearSales.delete(0, yearSales.length());
        
        	context.write(key, new Text(output));
 
	}

}