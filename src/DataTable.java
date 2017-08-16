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
import java.util.Set;

import javax.json.*;



public class DataTable {
	protected ArrayList<DataRow> dt;
	protected Set<String> columnas;
	public DataTable(){
		dt = new ArrayList();
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
				DataRow dr = new DataRow(row);
				dt.add(dr);				
			}
			jsr.close();
			columnas = dt.get(0).keys();
		} catch (FileNotFoundException e) {
			throw new Exception("No se ha podido parsear el DataTable correctamente");
		}
		
	}
	public DataTable (DataTable dtable){
		ArrayList<DataRow> dtrows = dtable.dt;
		DataRow patron = dtrows.get(0);
		dt = new ArrayList();
		columnas = patron.keys();
	}
	public void orderby(String key) throws Exception{
		orderby(0,dt.size()-1,key);
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
		result.columnas = new HashSet<String>();
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
	public DataTable Copy(){
		DataTable newdt = new DataTable();
		for ( int i = 0 ; i < dt.size() ; i++ ) {
			newdt.addRow(dt.get(i).Copy());
		}
		newdt.columnas = newdt.dt.get(0).keys();
		return newdt;
		
	}
	public DataRow row(int i){
		return dt.get(i);
	}
	public void fill(ResultSet rs) throws SQLException{
		while ( rs.next() ) {
			int numColumns = rs.getMetaData().getColumnCount();
			DataRow dr = new DataRow();
            for ( int i = 1 ; i <= numColumns ; i++ ) {
              
              String label = rs.getMetaData().getColumnLabel(i);
              dr.Add(label, rs.getObject(i));
              
            }
			dt.add(dr);
		}
		columnas = dt.get(0).keys();
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
	@Override
    public boolean equals(Object obj) {
        DataTable table = (DataTable)obj;
        boolean iguales =true;
        if(dt.size()!=table.RowCount()){
        	iguales=false;
        }
        else{
        	for ( int i = 0 ; i < dt.size() ; i++ ) {
        		iguales = iguales && dt.get(i).equals(table.row(i));
        		
        	}
        }
        return iguales;
        
        
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
	public DataRow NewRow(){
		DataRow dr = new DataRow();
		for(String key: columnas){
			dr.Add(key, new Object());
		}
		return dr;
	}
	
	
}
