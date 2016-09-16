package edu.tamu.isys.forecast.mapreduce;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.CSVReader;
//Class used for top product in each product sub category result
public class TopProductMap extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException, FileNotFoundException {
		Text keyToReducer = new Text();
				String line = value.toString();
				CSVReader salesReader = new CSVReader(new StringReader(line));
				String token[] = salesReader.readNext();  
				salesReader.close();
		      
				if(!token[16].equals("")){
					value.clear();
					//composite value of product name and quantity
					value.set(token[17] + "_" +token[4]);
					//Product sub category is set as key
					keyToReducer.set(token[16]);
					context.write(keyToReducer, value); 
				}
		
      }

}
