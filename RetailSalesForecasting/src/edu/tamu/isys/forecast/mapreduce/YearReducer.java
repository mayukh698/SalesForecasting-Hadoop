package edu.tamu.isys.forecast.mapreduce;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//YearReducer will be used for sales without province or produt category
public class YearReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
        	
			double sales = 0;
			double totalSales = 0;
			String[] line_values;
        	for (Text value: values) {
        	
        		line_values = value.toString().split(":");
        		sales = Float.parseFloat(line_values[1]);
        		totalSales=totalSales + sales;
          }
        	
        
        	String output = "Total sales:"+String.format("%.2f",totalSales);
        	context.write(key, new Text(output));
 
	}

}