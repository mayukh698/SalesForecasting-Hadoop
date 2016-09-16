package edu.tamu.isys.forecast.mapreduce;
import java.io.IOException;
import java.io.StringReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.opencsv.CSVReader;
//DefaultMapper is used for default result (Gross monthly,quarterly 
			//or yearly results)
public class DefaultMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		Text keyToReducer = new Text();
			String line = value.toString();
			CSVReader salesReader = new CSVReader(new StringReader(line));
			String[] ParsedLine = salesReader.readNext();
			salesReader.close();
			//to check if there is a order date in dataset
			if(!ParsedLine[2].equals("")){
				value.clear();
				String[] monthYear = ParsedLine[2].split("-");
				//Composite value of order month and sales
				value.set(monthYear[1]+":"+ParsedLine[5]);
				//Order year is set as key 
				keyToReducer.set(monthYear[2]);
				context.write(keyToReducer, value);
			}
	}	
}
