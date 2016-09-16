package edu.tamu.isys.forecast.mapreduce;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.CSVReader;
//CustomerMap is used for customer loyalty offer result
public class CustomerMap extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException, FileNotFoundException {
		   Text keyToReducer = new Text();		
	        	String line = value.toString();
	        	CSVReader salesReader = new CSVReader(new StringReader(line));
	        	String token[] = salesReader.readNext();  
	        	salesReader.close();
	        	if(!token[11].equals(""))
	        	{
	        		value.clear();
	        		value.set(token[15] + "_" +token[5]);
	        		keyToReducer.set(token[11]);
	        		//key is the customer name and value(composite)
	        		//is product category and sales
	        		context.write(keyToReducer, value); 
		        }
            }

}
