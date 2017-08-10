import java.sql.Time;
import java.sql.Timestamp;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

import javax.json.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Date;



public class DataRow {
	 protected Hashtable <String, Object> dr;
	 public DataRow(){
		 dr = new  Hashtable <String, Object>();
		 
	 }
	 public DataRow(DataRow row){
		 dr = new  Hashtable <String, Object>();
		 Iterator<String> keys = row.keys().iterator();
		 while(keys.hasNext()){
			 String key = keys.next();
			 dr.put(key, new Object());
			 
		 }
	 }
	 public DataRow Copy(){
		 DataRow drnew = new DataRow();
		 Iterator<String> keys = keys().iterator();
		 while(keys.hasNext()){
			 String key = keys.next();
			 drnew.Add(key, dr.get(key));
			 
		 }
		 return drnew;
	 }
	 public DataRow(JsonArray array) throws ClassNotFoundException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException{
		 dr = new  Hashtable <String, Object>();
		 for ( int i = 0 ; i < array.size() ; i++ ) {
			 JsonObject campo = array.getJsonObject(i);
			 String key = campo.getString("Key");
			 String tipo = campo.getString("Tipo");
			 String valor = campo.getString("Valor");
			 Class<?> cls = Class.forName(tipo);
			 int j=0;
			 Constructor<?>[] ctors = cls.getConstructors();
			 boolean encontrado=false;
			 Constructor<?> ctor=null;
			 while(j<ctors.length && !encontrado){
				 ctor = ctors[j];
				 Class<?>[] paramTypes = ctor.getParameterTypes();
				 if(paramTypes.length==1 && paramTypes[0].getSimpleName().compareTo("String")==0){
					 encontrado=true;
				 }
				 j++;
			 }
			 if(encontrado){
				 Object o = ctor.newInstance(valor);
				 dr.put(key, o);
			 }
			 else{
				 Method method = cls.getMethod("valueOf", String.class);
				 Object o = method.invoke("", valor);
				 dr.put(key, o);
				 
			 }
			 
			 
		 }
		 
	 }
	 public void Add(String name,Object value){
		 dr.put(name, value);
	 }
	 public void Add(int pos,Object value){
		 dr.put(dr.keySet().toArray()[pos].toString(),value);
		 
	 }
	 public Object get(String name){
		 return dr.get(name);
	 }
	 public Object get(int pos){
		 String name = dr.keySet().toArray()[pos].toString();
		 return dr.get(name);
	 }
	 public String get_String(String name){
		 return dr.get(name).toString();
	 }
	 public String get_String(int pos){
		 String name = dr.keySet().toArray()[pos].toString();
		 return dr.get(name).toString();
	 }
	 public int get_int(String name){
		 return ((Integer) dr.get(name)).intValue();
	 }
	 public int get_int(int pos){
		 String name = dr.keySet().toArray()[pos].toString();
		 return ((Integer) dr.get(name)).intValue();
	 }
	 public double get_double(String name){
		 
		 //return ((Double) dr.get(name)).doubleValue();
		 return new Double (dr.get(name).toString());
	 }
	 public double get_double(int pos){
		 
		 //return ((Double) dr.get(name)).doubleValue();
		 String name = dr.keySet().toArray()[pos].toString();
		 return new Double (dr.get(name).toString());
	 }
	 public Date get_date(String name){
		 //return new Date(dr.get(name).toString());
		 return Date.valueOf(dr.get(name).toString());
	 }
	 public Date get_date(int pos){
		 //return new Date(dr.get(name).toString());
		 String name = dr.keySet().toArray()[pos].toString();
		 return Date.valueOf(dr.get(name).toString());
	 }
	 public Time get_time(String name){
		 return Time.valueOf(dr.get(name).toString());
	 }
	 public Time get_time(int pos){
		 String name = dr.keySet().toArray()[pos].toString();
		 return Time.valueOf(dr.get(name).toString());
	 }
	 public Timestamp get_timestamp(String name){
		 return Timestamp.valueOf(dr.get(name).toString());
	 }
	 public Timestamp get_timestamp(int pos){
		 String name = dr.keySet().toArray()[pos].toString();
		 return Timestamp.valueOf(dr.get(name).toString());
	 }
	 public void writeJson(JsonObjectBuilder jo,int i) throws ClassNotFoundException{
	     JsonArrayBuilder jsa = Json.createArrayBuilder();
		 for(String key: dr.keySet()){
			 JsonObjectBuilder col = Json.createObjectBuilder();
			 col.add("Key", key);
			 col.add("Tipo", dr.get(key).getClass().getName());
			 if(dr.get(key).getClass().getSimpleName()=="Double"){
				 col.add("Valor", get_double(key));
			 }
			 else if(dr.get(key).getClass().getSimpleName()=="Integer"){
				 col.add("Valor", get_int(key));
			 }
			 else{
				 col.add("Valor", dr.get(key).toString()); 
			 
		 	 }
			 jsa.add(col);
		 }
		 jo.add("Row"+i, jsa);		 
	 }
	 public int Count(){
		 return dr.size();
	 }
	 @Override
	 public boolean equals(Object obj){
		 DataRow row = (DataRow)obj;
		 boolean iguales=true;
		 for(String key: dr.keySet()){
			 String origen=dr.get(key).toString();
			 String destino= row.get_String(key);
			 iguales = iguales && origen.compareTo(destino)==0;
		 }
		 return iguales;
		 
	 }
	 public Set<String> keys(){
		 return dr.keySet();
	 }
	 public Object CompareTo(DataRow dri,String key) throws Exception{
		 try {
			 Class<?> cls = get(key).getClass();
			 Method method = cls.getMethod("compareTo", Object.class);
			 return method.invoke(get(key),dri.get(key));
		 } catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				throw new Exception("Tipo de columna no permite ser ordenado");
		 } catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				throw new Exception("Tipo de columna no permite ser ordenado");
		 } catch (InvocationTargetException e) {
				// TODO Auto-generated catch block
				throw new Exception("Tipo de columna no permite ser ordenado");
		 } catch (SecurityException e) {
				// TODO Auto-generated catch block
				throw new Exception("Tipo de columna no permite ser ordenado");
		 } catch (NoSuchMethodException e) {
				// TODO Auto-generated catch block
				throw new Exception("Tipo de columna no permite ser ordenado");
		 }
	 }
}
