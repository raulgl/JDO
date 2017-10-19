import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.json.*;



public class DataTable {
	protected ArrayList<DataRow> dt;
	protected LinkedHashSet<String> columnas;
	public DataTable(){
		dt = new ArrayList();
		columnas = new LinkedHashSet<String>();
	}
	public DataTable(String path) throws Exception{
		File f = new File(path);
		FileInputStream fis;
		dt = new ArrayList();
		try {
			fis = new FileInputStream(f);
			JsonReader jsr = Json.createReader(fis);
			JsonObject jobj = jsr.readObject();
			for ( int i = 0 ; i < jobj.keySet().size() ; i++ ) {
				JsonArray row = (JsonArray) jobj.get("Row"+i);
				if(i==0) {
					columnas = obtener_columnas(row);
				}
				DataRow dr = new DataRow(row,columnas);
				dt.add(dr);				
			}
			jsr.close();
			
		} catch (FileNotFoundException e) {
			throw new Exception("No se ha podido parsear el DataTable correctamente");
		}
		
	}
	public static LinkedHashSet<String> obtener_columnas(JsonArray array){
		LinkedHashSet<String> cols = new LinkedHashSet<String>();
		for ( int i = 0 ; i < array.size() ; i++ ) {
			 JsonObject campo = array.getJsonObject(i);
			 String key = campo.getString("Key");
			 cols.add(key);
		}
		return cols;
	}
	public DataTable (DataTable dtable){
		ArrayList<DataRow> dtrows = dtable.dt;
		DataRow patron = dtrows.get(0);
		dt = new ArrayList();
		columnas = dtable.columnas;
	}
	public void orderby(String key) throws Exception{
		orderby(0,dt.size()-1,key);
	}
	public void orderby(String[] keys) throws Exception{
		orderby(0,dt.size()-1,keys);
	}
	public void orderby(int o,int N,String key) throws Exception{
		
		int i = o, j = N;
		int middle = o + (N-o)/2;
		DataRow pivot = dt.get(middle);
        while (i <= j) {
        	DataRow dri = dt.get(i);
        	Integer m = (Integer)pivot.CompareTo(dri,key);
            while (m>0) {
                i++;
                dri = dt.get(i);
            	m = (Integer)pivot.CompareTo(dri,key);
                
            }
            DataRow drj = dt.get(j);
            m = (Integer)pivot.CompareTo(drj,key);
            while (m<0) {
                j--;
                drj = dt.get(j);
                m = (Integer)pivot.CompareTo(drj,key);
            }
            if (i <= j) {
            	swap(i, j);
                i++;
                j--;
            }
        }
        if (o < j)
        	orderby(o, j,key);
        if (i <N)
        	orderby(i, N,key);
	}
	public void orderby(int o,int N,String[] keys) throws Exception{
		int i = o, j = N;
		int middle = o + (N-o)/2;
		DataRow pivot = dt.get(middle);
        while (i <= j) {
        	DataRow dri = dt.get(i);
        	Integer m = (Integer)pivot.CompareTo(dri,keys);
            while (m>0) {
                i++;
                dri = dt.get(i);
            	m = (Integer)pivot.CompareTo(dri,keys);
                
            }
            DataRow drj = dt.get(j);
            m = (Integer)pivot.CompareTo(drj,keys);
            while (m<0) {
                j--;
                drj = dt.get(j);
                m = (Integer)pivot.CompareTo(drj,keys);
            }
            if (i <= j) {
            	swap(i, j);
                i++;
                j--;
            }
        }
        if (o < j)
        	orderby(o, j,keys);
        if (i <N)
        	orderby(i, N,keys);
	}
	
	public void swap(int i,int j) throws Exception{
		
		DataRow dri = dt.get(i);
		DataRow drj = dt.get(j);
		dt.set(i, dt.get(j));
		dt.set(j, dri);
		dri = dt.get(i);
		drj = dt.get(j);
		
		
	}
	public DataTable join (DataTable dt,String key) throws Exception{
		
		DataTable result = new DataTable();
		dt.orderby(key);
		//dt.writeJson("C:/prueba.json");
		orderby(key);
		int j = 0;
		int i = 0;
		while(i<RowCount() && j<dt.RowCount()){
			DataRow dri = row(i);
			DataRow drj = dt.row(j);
			Integer m = (Integer)drj.CompareTo(dri,key);
			if(m==0){
				int k=j;
				DataRow drk = dt.row(k);
				while(k<dt.RowCount() && m==0){
					DataRow dr = new DataRow(dri,drk,key);
					result.addRow(dr);
					k++;
					if(k<dt.RowCount()){
						drk = dt.row(k);
						m = (Integer)drk.CompareTo(dri,key);
					}
					
										
				}
				//i++;
			}
			if(m<0){
				j++;
			}
			else{
				i++;
			}
		}
		result.columnas = new LinkedHashSet<String>();
		Iterator<String> keys = columnas.iterator();
		 while(keys.hasNext()){
			 String llave = keys.next();
			 result.columnas.add(llave);
		 }
		 keys = dt.columnas.iterator();
		 while(keys.hasNext()){
			 String llave = keys.next();
			 if(llave.compareTo(key)!=0){
				 result.columnas.add(llave);
			 }
		 }
		return result;
	}
	public DataTable join (DataTable dt,String[] keys) throws Exception{
		DataTable result = new DataTable();
		dt.orderby(keys);
		//dt.writeJson("C:/prueba.json");
		orderby(keys);
		result.columnas = new LinkedHashSet<String>();
		Iterator<String> llaves = columnas.iterator();
		while(llaves.hasNext()){
			 String llave = llaves.next();
			 result.columnas.add(llave);
		}
		llaves = dt.columnas.iterator();
		while(llaves.hasNext()){
			 String llave = llaves.next();
			 if(!Funciones.buscar(keys,llave)){
				 result.columnas.add(llave);
			 }
		}
		int j = 0;
		int i = 0;
		while(i<RowCount() && j<dt.RowCount()){
			DataRow dri = row(i);
			DataRow drj = dt.row(j);
			Integer m = (Integer)drj.CompareTo(dri,keys);
			if(m==0){
				int k=j;
				DataRow drk = dt.row(k);
				while(k<dt.RowCount() && m==0){
					DataRow dr = new DataRow(dri,drk,keys);
					dr.columnas= result.columnas;
					result.addRow(dr);
					k++;
					if(k<dt.RowCount()){
						drk = dt.row(k);
						m = (Integer)drk.CompareTo(dri,keys);
					}
					
										
				}
				//i++;
			}
			if(m<0){
				j++;
			}
			else{
				i++;
			}
		}
		
		return result;
	}
	public DataTable Copy() throws Exception{
		DataTable newdt = new DataTable();
		for ( int i = 0 ; i < dt.size() ; i++ ) {
			newdt.addRow(dt.get(i).Copy());
		}
		Iterator<String> llaves = columnas.iterator();
		 while(llaves.hasNext()){
			 String llave = llaves.next();
			 newdt.columnas.add(llave);
		 }
		
		return newdt;
		
	}
	public DataRow row(int i){
		return dt.get(i);
	}
	public void fill(ResultSet rs) throws Exception{
		int numColumns = rs.getMetaData().getColumnCount();
		columnas = new LinkedHashSet<String>();
		for ( int i = 1 ; i <= numColumns ; i++ ) {
			String label = rs.getMetaData().getColumnLabel(i);
			columnas.add(label);
		}
		while ( rs.next() ) {
			numColumns = rs.getMetaData().getColumnCount();
			DataRow dr = new DataRow(columnas);
			
            for ( int i = 1 ; i <= numColumns ; i++ ) {
              
              String label = rs.getMetaData().getColumnLabel(i);
              dr.Add(label, rs.getObject(i));              
            }
			dt.add(dr);
		}
		
		
	}
	public void writeJson(String path) throws Exception{
		JsonObjectBuilder jo = Json.createObjectBuilder();
		 for ( int i = 0 ; i < dt.size() ; i++ ) {
			 dt.get(i).writeJson(jo,i);
		 }
		 JsonObject json = jo.build();
		 JsonWriter jsonWriter;
		try {
			jsonWriter = Json.createWriter(new FileWriter(path));
			jsonWriter.writeObject(json);
			jsonWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new Exception("No se ha podido guardar el DataTable.Por favor revise permisos");
		}
		
		 
	}
	public void writeCSV(String path) throws Exception {
		 try {
			FileWriter writer = new FileWriter(path);
			Iterator<String> llaves = columnas.iterator();
			 while(llaves.hasNext()){
				 String llave = llaves.next();
				 writer.append(llave);
				 writer.append(";");
			 }
			 writer.append("\n");
			for ( int i = 0 ; i < dt.size() ; i++ ) {
				writer.append(dt.get(i).writeCSV());
				writer.append("\n");
			}
			writer.flush();
	        writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new Exception("El path introducido es erroneo");
		}
		 
	}
	@Override
    public boolean equals(Object obj) {
        DataTable table = (DataTable)obj;
        boolean iguales =true;
        if(dt.size()!=table.RowCount()){
        	iguales=false;
        }
        else{
        	int i=0;
        	while(i < dt.size() && iguales) {        	
        		iguales = iguales && dt.get(i).equals(table.row(i));
        		i++;        		
        	}
        }
        return iguales;
        
        
    }
	public void add_columna(String columna) {
		columnas.add(columna);
	}
	public int RowCount(){
		return dt.size();
	}
	public int ColumCount(){
		return columnas.size();
	}
	public void addRow(DataRow dr){
		dt.add(dr);		
	}
	public DataRow NewRow() throws Exception{
		DataRow dr = new DataRow(columnas);
		Object[] array = columnas.toArray();
		int i=0;
		while(i<array.length) {
			String key = array[i].toString();
			dr.Add(key, new Object());
			i++;
		}
		return dr;
	}
	
	
}
