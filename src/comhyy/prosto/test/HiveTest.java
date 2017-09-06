package comhyy.prosto.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveTest {

	public static void main(String[] args) throws Exception {
		String s=null;
		s.indexOf("1");
		String sql="select * ,row_number() over() num from student_infos ";
		  Class.forName("com.facebook.presto.jdbc.PrestoDriver");
	        Connection con = DriverManager.getConnection("jdbc:presto://spark1:8080/hive/default", "admin", "");
	        try {
	          Statement stmt = con.createStatement();
//	          ResultSet resCount = stmt.executeQuery(slqCount.toString());
//	         if(resCount.next()){
//	             Integer pageSize = Integer.parseInt(resCount.getString(1));
//	             atpco.setPageSize(pageSize);
//	         }
	          ResultSet res = stmt.executeQuery(sql);
	          
	          ResultSetMetaData  metaData=res.getMetaData();
	          
	          for(int i=0;i<metaData.getColumnCount();i++){
	        	  int key=i+1;
	        	  String value = metaData.getColumnName(key);
	        	  System.out.print("|"+key+":"+value+"|");
	          }
	  
	          System.out.println();

	          System.out.println("-------------------");
	          
	          while(res.next()){    
	              List<Map<String,String>> resultList=new ArrayList<Map<String,String>>();
	              for(int i=1;i<=metaData.getColumnCount();i++){
	                  Map<String,String> map=new HashMap<String,String>();
	                 
	                  String row = metaData.getColumnName(i);
	                  String rowValue = res.getString(i);
	                  map.put(row, rowValue);
	                  resultList.add(map);
	                  
	                  System.out.println(row+":"+rowValue);
	              }
	              
	          }
	          
	        } catch (SQLException e) {
	            e.printStackTrace();
	        } 
	        finally {
	            
	            con.close();
	        }

	}
}
