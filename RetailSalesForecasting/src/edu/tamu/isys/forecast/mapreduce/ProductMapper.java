package edu.tamu.isys.forecast.mapreduce;
import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.CSVReader;
//ProductMapper will be used for results based on product category
public class ProductMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Text keyToReducer = new Text();
				String line = value.toString();
				CSVReader salesReader = new CSVReader(new StringReader(line));
				String[] ParsedLine = salesReader.readNext();
				salesReader.close();
				if(!ParsedLine[15].equals("")){
					value.clear();
					//Composite value of order date and sales
					value.set(ParsedLine[2] + "::" +ParsedLine[5]);
					//Product cateogory is set as key
					keyToReducer.set(ParsedLine[15]);
					context.write(keyToReducer, value);
				}
	}	
}
