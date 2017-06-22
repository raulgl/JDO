import java.util.Dictionary;
import java.util.Hashtable;




public class DataRow {
	 Hashtable <String, Object> dr;
	 public DataRow(){
		 dr = new  Hashtable <String, Object>();
		 
	 }
	 public void Add(String name,Object value){
		 dr.put(name, value);
	 }
	 public String get_String(String name){
		 return dr.get(name).toString();
	 }
	 public int get_int(String name){
		 return ((Integer) dr.get(name)).intValue();
	 }
	 public double get_double(String name){
		 return ((Double) dr.get(name)).doubleValue();
	 }
}
