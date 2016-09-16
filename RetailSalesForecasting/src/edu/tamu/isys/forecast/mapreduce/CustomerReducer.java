package edu.tamu.isys.forecast.mapreduce;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//CustomerReducer is used for Customer loyalty offer result
public class CustomerReducer extends Reducer<Text, Text, Text, Text>{


	public void reduce(Text key, Iterable<Text> values, Context context)
	          throws IOException, InterruptedException {
		   
		 		float totalSales=0;
		 		String topProduct="";
		 		HashMap<String, Float> mapped = new HashMap<String, Float>(); 
		 		Map<String, String> discountMap = new TreeMap<String, String>();
		 		//Map used to store loyalty offer - starts
		 		discountMap.put("Technology", "Get 50% off in Technology");
		 		discountMap.put("Furniture", "Buy 1 get 40 % discount on 2nd product");
		 		discountMap.put("Office Supplies", "Get 30% off in Technology");
		 		//Map used to store loyalty offer - ends
		 		for (Text compositeSales : values) 
		 		{
	         		String tempCompositeSales=compositeSales.toString();
					String[] temp = tempCompositeSales.split("_");
					String product = temp[0];
					float totalFigure = Float.parseFloat(temp[1]); 
					if (!(product.equals("") || product.isEmpty() 
							|| product == null)|| product== "") 						
					{   
						totalSales=0;
						if (mapped.containsKey(product)) 
					    {		
					        float oldQuantity = mapped.get(product);
					   
					        totalSales=oldQuantity+totalFigure;
					        mapped.put(product, totalSales);
					    }else								    
					    {  
					    	totalSales=totalFigure;
							mapped.put(product, totalSales);
					    }                				 
		            }
		 		}
		 		float  maxCount = 0;
		 		Set<Entry<String, Float>> entry = mapped.entrySet();
		 		//Loop used to get the top product category for each customer
		 		for (Entry<String, Float> ent : entry) 
		 		{
		 			float count = mapped.get(ent.getKey());
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
		 			//loyalty description added to value
		 			topProduct = topProduct +","+ discountMap.get(topProduct);
		 			topValue.set(topProduct); 
		 			if(!("unknown".equals(topProduct) || "unknown".equals(key)))
				    {
		 				context.write(topKey, topValue);
				    }
				}

		
		}
}
	
