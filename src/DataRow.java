import java.sql.Time;
import java.sql.Timestamp;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.json.*;
import javax.json.stream.JsonGenerator;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Date;



public class DataRow {
	 protected Hashtable <String, Object> dr;
	 protected LinkedHashSet<String> columnas;
	 public DataRow(LinkedHashSet<String> colums){
		 dr = new  Hashtable <String, Object>();
		 columnas = colums;
		 
		 
	 }
	 public DataRow(DataRow row){
		 dr = new  Hashtable <String, Object>();
		 Iterator<String> keys = row.keys().iterator();
		 while(keys.hasNext()){
			 String key = keys.next();
			 dr.put(key, new Object());
			 
		 }
		 columnas = row.columnas;
	 }
	 public DataRow Copy() throws Exception{
		 DataRow drnew = new DataRow(columnas);
		 Iterator<String> keys = keys().iterator();
		 while(keys.hasNext()){
			 String key = keys.next();
			 drnew.Add(key, dr.get(key));
			 
		 }
		 drnew.columnas = columnas;
		 return drnew;
	 }
	 public DataRow(JsonArray array,LinkedHashSet<String> cols) throws ClassNotFoundException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException{
		 columnas = cols;
		 dr = new  Hashtable <String, Object>();
		 for ( int i = 0 ; i < array.size() ; i++ ) {
			 JsonObject campo = array.getJsonObject(i);
			 String key = campo.getString("Key");
			 String tipo = campo.getString("Tipo");
			 String valor = campo.getString("Valor");
			 if(tipo.compareTo("java.lang.Object")==0) {
				 dr.put(key, new Object());
			 }
			 else if(tipo.compareTo("java.lang.String")==0) {
				 dr.put(key, valor);
			 }
			 else {
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
		 
	 }
	 public void set_columna(LinkedHashSet<String> colums) {
		 columnas = colums;
	 }
	 public DataRow (DataRow dri,DataRow drj,String key){
		 dr = new  Hashtable <String, Object>();
		 Iterator<String> keys = dri.columnas.iterator();
		 while(keys.hasNext()){
			 String llave = keys.next();
			 dr.put(llave, dri.get(llave));
			 
		 }
		 keys = drj.columnas.iterator();
		 while(keys.hasNext()){
			 String llave = keys.next();
			 if(llave.toUpperCase().compareTo(key.toUpperCase())!=0){
				 dr.put(llave, drj.get(llave));
			 }
			 
		 }
		 
	 }
	 public DataRow (DataRow dri,DataRow drj,String[] key){
		 dr = new  Hashtable <String, Object>();
		 Iterator<String> keys = dri.columnas.iterator();
		 while(keys.hasNext()){
			 String llave = keys.next();
			 dr.put(llave, dri.get(llave));
			 
		 }
		 keys = drj.columnas.iterator();
		 while(keys.hasNext()){
			 String llave = keys.next();
			 if(!Funciones.buscar(key, llave)){
				 dr.put(llave, drj.get(llave));
			 }
			 
		 }
		 
	 }
	 public void Add(String name,Object value) throws Exception{
		 if(columnas.contains(name)) {
			 if(value!=null){
				 dr.put(name, value);
			 }
			 else {
				 dr.put(name, new Object());
			 }
		 }
		 else {
			 throw new Exception("El nombre de la columna no es correcto");
		 }
	 }
	 public void Add(int pos,Object value){
		 if(value!=null){
			 dr.put(columnas.toArray()[pos].toString(),value);
		 }
		 else {
			 dr.put(columnas.toArray()[pos].toString(),new Object());
		 }
		 
	 }
	 public Object get(String name){
		 return dr.get(name);
	 }
	 public Object get(int pos){
		 String name = columnas.toArray()[pos].toString();
		 return dr.get(name);
	 }
	 public String get_String(String name){
		 if(dr.containsKey(name)) {
			 return dr.get(name).toString();
		 }
		 else {
			 return null;
		 }
	 }
	 public String get_String(int pos){
		 
		 String name = columnas.toArray()[pos].toString();
		 if(dr.containsKey(name)) {
			 return dr.get(name).toString();
		 }
		 else {
			 return null;
		 }
	 }
	 public Integer get_int(String name){
		 return (Integer) dr.get(name);
	 }
	 public Integer get_int(int pos){
		 String name = columnas.toArray()[pos].toString();
		 return ((Integer) dr.get(name));
	 }
	 public Double get_double(String name){
		 
		 //return ((Double) dr.get(name)).doubleValue();
		 if(dr.get(name)!=null){
			 return ((Double) dr.get(name)).doubleValue();
		 }
		 else{
			 return Double.NaN;
		 }
	 }
	 public Double get_double(int pos){
		 
		 //return ((Double) dr.get(name)).doubleValue();
		 String name = columnas.toArray()[pos].toString();
		 if(dr.get(name)!=null){
			 return new Double (dr.get(name).toString());
		 }
		 else{
			 return Double.NaN;
		 }
	 }
	 public Date get_date(String name){
		 //return new Date(dr.get(name).toString());
		 if(dr.get(name)!=null){
			 return Date.valueOf(dr.get(name).toString());
		 }
		 else{
			 return Date.valueOf("0000-00-00");
		 }
	 }
	 public Date get_date(int pos){
		 //return new Date(dr.get(name).toString());
		 String name = columnas.toArray()[pos].toString();
		 if(dr.get(name)!=null){
			 return Date.valueOf(dr.get(name).toString());
		 }
		 else{
			 return null;
		 }
	 }
	 public Time get_time(String name){
		 if(dr.get(name)!=null){
			 return Time.valueOf(dr.get(name).toString());
		 }
		 else{
			 return null;
		 }
	 }
	 public Time get_time(int pos){
		 String name = columnas.toArray()[pos].toString();
		 if(dr.get(name)!=null){
			 return Time.valueOf(dr.get(name).toString());
		 }
		 else{
			 return null;
		 }
	 }
	 public Timestamp get_timestamp(String name){
		 if(dr.get(name)!=null){
			 return Timestamp.valueOf(dr.get(name).toString());
		 }
		 else{
			 return null;
		 }
	 }
	 public Timestamp get_timestamp(int pos){
		 String name = columnas.toArray()[pos].toString();
		 if(dr.get(name)!=null){
			 return Timestamp.valueOf(dr.get(name).toString());
		 }
		 else{
			 return null;
		 }
	 }
	 public void writeJson(JsonGenerator jgen,int i) throws ClassNotFoundException{
		 jgen.writeStartArray("Row"+i);
	     Iterator<String> keys = columnas.iterator();
		 while(keys.hasNext()){
			 String key = keys.next();
			 jgen.writeStartObject();
			 jgen.write("Key", key);
			 jgen.write("Tipo", dr.get(key).getClass().getName());
			 if(dr.get(key).getClass().getSimpleName().compareTo("Double")==0){
				 jgen.write("Valor", get_double(key).toString());
			 }
			 else if(dr.get(key).getClass().getSimpleName().compareTo("Integer")==0){
				 jgen.write("Valor", get_int(key).toString());
			 }
			 else{
				 jgen.write("Valor", dr.get(key).toString()); 
			 
		 	 }
			 jgen.writeEnd();
		 }
		 jgen.writeEnd();	 
	 }
	 public String writeCSV() {
		 String CSV ="";
		 int i=0;
		 while(i<Count()) {
			 Object obj = get(i);
			 if(obj.getClass().getSimpleName().compareTo("Object")==0) {
				 CSV = CSV + get_String(i);
			 }
			 CSV = CSV + ";";
			 i++;
		 }
		 return CSV;
	 }
	 public int Count(){
		 return dr.size();
	 }
	 @Override
	 public boolean equals(Object obj){
		 DataRow row = (DataRow)obj;
		 boolean iguales=(row.Count()==Count());
		 if(iguales){
			 Iterator<String> keys = columnas.iterator();
			 while(keys.hasNext() && iguales){
				 String key = keys.next();
				 Object org = dr.get(key);
				 Object des = row.get(key);
				 if(org.getClass().getSimpleName().compareTo("Object")!=0 && des.getClass().getSimpleName().compareTo("Object")!=0) {
					 String origen=dr.get(key).toString();
					 String destino= row.get_String(key);
					 iguales = iguales && origen.compareTo(destino)==0;
					 if(!iguales) {
						 @SuppressWarnings("unused")
						 String error= "no son iguales"; 
					 }
				 }
			 }
		 }
		 return iguales;
		 
	 }
	 public Set<String> keys(){
		 return columnas;
	 }
	 public Object CompareTo(DataRow dri,String key) throws Exception{
		 try {
			 if(get(key)==null){
				 
			 }
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
	 public Object CompareTo(DataRow dri,String[] keys) throws Exception{
		 try {
			 Integer m =0;
			 int i=0;
			 while(m==0 && i<keys.length){
				 String key = keys[i];
				 Class<?> cls = get(key).getClass();
				 Method method = cls.getMethod("compareTo", Object.class);
				 m =  (Integer) method.invoke(get(key),dri.get(key));
				 
				 i++;
			 }
		 return m;	 
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
