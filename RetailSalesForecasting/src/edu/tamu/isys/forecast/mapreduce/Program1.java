package edu.tamu.isys.forecast.mapreduce;


import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Program1 {

	public static void main(String[] args) throws Exception {
		
		//Properties prop=new Properties();
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "forecast");
		int mapValue = 3;
		int reducerValue = 6;
		
		/*Path pt=new Path("hdfs://localhost:9000/home/user/mayuk_000/MyDataFolder/userinput.txt");
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;
        String[] splitLine;
        line=br.readLine();
        while ((line = br.readLine()) != null){
        	splitLine = line.split(",");
        	mapValue = new Integer(splitLine[0]);
        	reducerValue = new Integer(splitLine[1]);
        }	*/
		
	    if (mapValue==1 && reducerValue==6){
	    	job.setMapperClass(DefaultMapper.class);
			
			job.setReducerClass(QuarterReducer.class);
	    }
	    else if(mapValue==1 && reducerValue==7){
	    	job.setMapperClass(DefaultMapper.class);
			
			job.setReducerClass(MonthReducer.class);
	    }
	    else if(mapValue==1 && reducerValue==8){
	    	job.setMapperClass(DefaultMapper.class);
			
			job.setReducerClass(YearReducer.class);
	    }
		else if(mapValue==2 && reducerValue==6){
			job.setMapperClass(ProvinceMapper.class);
			
			job.setReducerClass(QuarterGrossReducer.class);   	
			    }
		else if(mapValue==2 && reducerValue==7){
			job.setMapperClass(ProvinceMapper.class);
			
			job.setReducerClass(MonthGrossReducer.class);
		}
		else if(mapValue==2 && reducerValue==8){
			job.setMapperClass(ProvinceMapper.class);
			
			job.setReducerClass(YearGrossReducer.class);
		}
		else if(mapValue==3 && reducerValue==6){
			job.setMapperClass(ProvinceMapper.class);
			
			job.setReducerClass(QuarterForecastReducer.class);
		}
		else if(mapValue==3 && reducerValue==7){
			job.setMapperClass(ProvinceMapper.class);
			
			job.setReducerClass(MonthForecastReducer.class);
		}
		else if(mapValue==3 && reducerValue==8){
			job.setMapperClass(ProvinceMapper.class);
			
			job.setReducerClass(YearForecastReducer.class);
		}
		else if(mapValue==4 && reducerValue==6){
			job.setMapperClass(ProductMapper.class);
			
			job.setReducerClass(QuarterGrossReducer.class);
		}
		else if(mapValue==4 && reducerValue==7){
			job.setMapperClass(ProductMapper.class);
			
			job.setReducerClass(MonthGrossReducer.class);
		}
		else if(mapValue==4 && reducerValue==8){
			job.setMapperClass(ProductMapper.class);
			
			job.setReducerClass(YearGrossReducer.class);
		}
		else if(mapValue==5 && reducerValue==6){
			job.setMapperClass(ProductMapper.class);
			
			job.setReducerClass(QuarterForecastReducer.class);
		}
		else if(mapValue==5 && reducerValue==7){
			job.setMapperClass(ProductMapper.class);
			
			job.setReducerClass(MonthForecastReducer.class);
		}
		else if(mapValue==5 && reducerValue==8){
			job.setMapperClass(ProductMapper.class);
			
			job.setReducerClass(YearForecastReducer.class);
		}
		else if(mapValue==9 && reducerValue==0){
			job.setMapperClass(TopProductMap.class);
			
			job.setReducerClass(TopProductReducer.class);
		}
		else if(mapValue==10 && reducerValue==0){
			job.setMapperClass(CustomerMap.class);
			
			job.setReducerClass(CustomerReducer.class);
		}
	    
	    //job.setMapOutputKeyClass(LongWritable.class);
	    //job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		
	}
}
