package edu.tamu.isys.forecast.mapreduce;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//Class used for monthly gross sales based on product category or province
public class MonthGrossReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
        	String[] line_values;
        	String output = "";
        	String orderDate = "";
        	String[] monthValue;
        	String orderDateYear = "";
        	String orderDateMonth = "";
        	String orderMonth = "";
        	double sales = 0;
        	StringBuffer monthSales = new StringBuffer();
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
        	for (Text value: values) {
        	//Splitting based on the sign "::" 
        		line_values = value.toString().split("::");
            
        		sales = Float.parseFloat(line_values[1]);
            
        		orderDate = line_values[0].toString();
        		monthValue = orderDate.split("-");
        		orderDateYear = monthValue[2];
        		orderDateMonth = monthValue[1];
        		orderMonth = orderDateYear+":"+orderDateMonth;
        		//salesYearMap will contain values based on year and month
        		if(salesYearMap.containsKey(orderMonth)){
        			sales = sales+salesYearMap.get(orderMonth);
        			salesYearMap.put(orderMonth, sales);
        		}
        		else {
        			salesYearMap.put(orderMonth, sales );
            	
        		}
                       
        	}
        	//Looping through salesYearMap to fetch sales values 
        	for (Entry<String, Double> entry : salesYearMap.entrySet()) {
        		String[] valueYear = entry.getKey().split(":");
        		String year = valueYear[0];
        		String month = valueYear[1];
        		monthSales = 
        			 monthSales.append(System.lineSeparator()).append(" Year ").append(year)
        			.append(" and month :").append(monthMap.get(month))
        			.append("-").append(" Total Sales: ")
        			.append(String.format("%.2f",entry.getValue())).append(".");
        	}
        	output = monthSales.toString();
        	monthSales.delete(0, monthSales.length());
        	context.write(key, new Text(output));
       
     }

}