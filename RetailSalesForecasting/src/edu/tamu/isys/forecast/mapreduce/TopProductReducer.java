package edu.tamu.isys.forecast.mapreduce;



import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//TopProductReducer class used for top product in each product sub category result
public class TopProductReducer extends Reducer<Text, Text, Text, Text>{
	 
	int totalQuantity=0;

	public void reduce(Text key, Iterable<Text> values, Context context)
	          throws IOException, InterruptedException {
		
		   		String topProduct="";
		   		HashMap<String, Integer> mapped = 
		   				new HashMap<String, Integer>(); 
	       		for (Text compositeProduct : values) 
	       		{
	            
					String tempCompositeProduct=compositeProduct.toString();
					//Splitting on "_" sign
					String[] temp = tempCompositeProduct.split("_");
					String product = temp[0];
					int orderQuantity = Integer.parseInt(temp[1]); 
					//storing order quantity for each product -starts
					if (!(product.equals("") || product.isEmpty() 
							|| product == null)|| product== "") 						
					{   
						totalQuantity=0;
						if (mapped.containsKey(product)) 
						{		
					        int oldQuantity = mapped.get(product);
					   
					        totalQuantity=oldQuantity+orderQuantity;
					        mapped.put(product, new Integer (totalQuantity));
						}else 
						{ 
							totalQuantity=orderQuantity;
							mapped.put(product, new Integer(totalQuantity));
						}                				
							   
		            }
					//storing order quantity for each product -ends
				}
				int  maxCount = 0;
				Set<Entry<String, Integer>> entry = mapped.entrySet();
				for (Entry<String, Integer> ent : entry) 
				 {
						int count = mapped.get(ent.getKey());
						if (count > maxCount) 
						{
							maxCount=count;
							topProduct = ent.getKey();
						}
				}
				if (topProduct != null) 
				{
						Text topKey = new Text();
						Text topValue = new Text();
						topKey.set(key);
						topProduct=topProduct.replace(",", " ");
						topValue.set(topProduct); //set key and value for final results
						if(!("unknown".equals(topProduct) 
								|| "unknown".equals(key)))
						{
						   context.write(topKey, topValue);
						}
				}
		
		
		}
}
	
