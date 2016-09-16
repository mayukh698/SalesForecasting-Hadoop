package edu.tamu.isys.forecast.managedbean;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Properties;
import javax.faces.bean.ManagedBean;
import javax.faces.context.ExternalContext;
import javax.faces.context.FacesContext;
import javax.servlet.http.HttpServletResponse;


//Class UserOption is created to interact with the user interface

@ManagedBean
public class UserOption {

	
	private int userOption ;
	public int getUserOption() {
		return userOption;
	}

	public void setUserOption(int userOption) {
		this.userOption = userOption;
	}

	public int getUserSubOption() {
		return userSubOption;
	}

	public void setUserSubOption(int userSubOption) {
		this.userSubOption = userSubOption;
	}

	private int userSubOption ;

	Properties prop;
	// This method is used to fetch the value from User Interface
	public String setOptionCombination() throws InterruptedException{
		BufferedWriter output = null;
		File file=null;
		File fileTemp=null;
		try {
			prop=new Properties();
			InputStream input=this.getClass().getClassLoader()
					.getResourceAsStream("config.properties");
			if(input!=null){
				prop.load(input);
			}

			String filePath=prop.getProperty("filePath");
			String fileName=prop.getProperty("fileName");
			  fileTemp=new File(filePath);
			 File[] directoryContents=fileTemp.listFiles();
			 for(int i=0;i<directoryContents.length;i++){
				 if(directoryContents[i].getName()==fileName){ 
					 directoryContents[i].delete();
					 file=new File(filePath+fileName);
					 FileWriter writer=new FileWriter(file);
			            output = new BufferedWriter(writer);
			            output.write(Integer.toString(this.getUserOption())
			            		+","+Integer.toString(userSubOption));
				 }
			 }
			     
			 	file=new File(filePath+fileName);
			 	FileWriter writer=new FileWriter(file);
	            output = new BufferedWriter(writer);
	            output.write(Integer.toString(this.getUserOption())
	            		+","+Integer.toString(userSubOption));
	            
	            //Code used to run bat file in cmd - Starts
	            ProcessBuilder pb = new ProcessBuilder("cmd", "/c",
	            		prop.getProperty("batFileLinkPath"));
	            Process p = pb.start();
	            p.waitFor();
	          //Code used to run bat file in cmd - Ends
	            
	        } catch ( IOException e ) {
	        	e.printStackTrace();
	        } finally {
	            if ( output != null )
					try {
						output.close();
					} catch (IOException e) {
						
					}
	            
	        }
		return "success";
	    }
	//The below method will display the result on JSP
	public String displayResult(){
		
		try {
			//To display the outfile 
			prop=new Properties();
			InputStream input=this.getClass().getClassLoader()
					.getResourceAsStream("config.properties");
			if(input!=null){
				prop.load(input);
			}
			//loading the path of the result file
			//Path can be changed from config.properties file
			File filReadMe = new File(prop.getProperty("resultFile")+"result.csv");
		    BufferedReader brReadMe = new BufferedReader
		             (new InputStreamReader(new FileInputStream(filReadMe)
		            		 , "UTF-8"));
		    ArrayList<String> lines = new ArrayList<String>();
		   
		    String line = null;
		    while ((line = brReadMe.readLine()) != null) {
		        lines.add(line);
		    }
			
			ExternalContext externalContext = 
					FacesContext.getCurrentInstance().getExternalContext();
		
			HttpServletResponse response = 
					(HttpServletResponse) externalContext.getResponse();
			response.setContentType("text/html");
			PrintWriter out = response.getWriter();
		
			brReadMe.close();
			//Following code is used to generate a HTML table 
		    out.println("<html>");
		    out.println("<body>");
		    out.println("<CENTER>");
		    out.println("<button type='button' align='right' name='back' onclick='history.back()'>Go back to main page</button>");
		   
		    out.println("<table BORDER=1 CELLPADDING=0 CELLSPACING=0 WIDTH=50% ALIGN=CENTER>");
		    for (String eachLine : lines){
		         out.println("<tr>");
		         String[] splitLine = eachLine.split(",");
		         for (String lineContent : splitLine){
		             out.println("<td>");
		             out.println(lineContent);        
		             out.println("</td>");
		         }
		         
		         out.println("</tr>");
		    }
		    out.println("</table>");
		    out.println("</body></html>");
		    FacesContext.getCurrentInstance().responseComplete(); 
		
		} catch (IOException e) {e.printStackTrace();}
		
		return "success";
		}
	}

