package edu.tamu.isys.forecast.mapreduce;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//Hadoop execution will be done in Program class
public class Program {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ",");
		Job job = Job.getInstance(conf, "forecast");
		//Code used to read user input from command line - Starts
		System.out.println("Enter 1 for Product category forecast - monthly");
		System.out.println("Enter 2 for Product category forecast - quarterly");
		System.out.println("Enter 3 for Product category forecast - yearly");
		System.out.println("Enter 4 for Customer loyalty offers");
		System.out.println("Enter 5 for Top products in each sub category");
		System.out.println("Enter 6 for Province forecast - monthly");
		System.out.println("Enter 7 for Province forecast - quarterly");
		System.out.println("Enter 8 for Province forecast - yearly");
		System.out.println("Enter 9 for Product gross - monthly");
		System.out.println("Enter 10 for Product gross - quarterly");
		System.out.println("Enter 11 for Product gross - yearly");
		System.out.println("Enter 12 for Province gross - monthly");
		System.out.println("Enter 13 for Province gross - quarterly");
		System.out.println("Enter 14 for Province gross - yearly");
		System.out.println("Enter 15 for gross monthly sales");
		System.out.println("Enter 16 for gross quarterly sales");
		System.out.println("Enter 17 for gross yearly sales");
		//Code used to read user input from command line - Ends
		Scanner scanner = new Scanner(System.in);
		int choice = scanner.nextInt();
		//Setting mapper reducer based on user input - starts
		if(choice == 1){
			job.setMapperClass(ProductMapper.class);
			job.setReducerClass(MonthForecastReducer.class);
		}
		else if(choice == 2){
			job.setMapperClass(ProductMapper.class);
			job.setReducerClass(QuarterForecastReducer.class);
		}
		else if(choice == 3){
			job.setMapperClass(ProductMapper.class);
			job.setReducerClass(YearForecastReducer.class);
		}
		else if(choice == 4){
			job.setMapperClass(CustomerMap.class);
			job.setReducerClass(CustomerReducer.class);
		}
		else if(choice == 5){
			job.setMapperClass(TopProductMap.class);
			job.setReducerClass(TopProductReducer.class);
		}
		else if(choice == 6){
			job.setMapperClass(ProvinceMapper.class);
			job.setReducerClass(MonthForecastReducer.class);
		}
		else if(choice == 7){
			job.setMapperClass(ProvinceMapper.class);
			job.setReducerClass(QuarterForecastReducer.class);
		}
		else if(choice == 8){
			job.setMapperClass(ProvinceMapper.class);
			job.setReducerClass(YearForecastReducer.class);
		}
		else if(choice == 9){
			job.setMapperClass(ProductMapper.class);
			job.setReducerClass(MonthGrossReducer.class);
		}
		else if(choice == 10){
			job.setMapperClass(ProductMapper.class);
			job.setReducerClass(QuarterGrossReducer.class);
		}
		else if(choice == 11){
			job.setMapperClass(ProductMapper.class);
			job.setReducerClass(YearGrossReducer.class);
		}
		else if(choice == 12){
			job.setMapperClass(ProvinceMapper.class);
			job.setReducerClass(MonthGrossReducer.class);
		}
		else if(choice == 13){
			job.setMapperClass(ProvinceMapper.class);
			job.setReducerClass(QuarterGrossReducer.class);
		}
		else if(choice == 14){
			job.setMapperClass(ProvinceMapper.class);
			job.setReducerClass(YearGrossReducer.class);
		}
		else if(choice == 15){
			job.setMapperClass(DefaultMapper.class);
			job.setReducerClass(MonthReducer.class);
		}
		else if(choice == 16){
			job.setMapperClass(DefaultMapper.class);
			job.setReducerClass(QuarterReducer.class);
		}
		else if(choice == 17){
			job.setMapperClass(DefaultMapper.class);
			job.setReducerClass(YearReducer.class);
		}
		//Setting mapper reducer based on user input - ends
		scanner.close();
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		
	}
}
