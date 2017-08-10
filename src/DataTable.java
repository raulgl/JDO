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
        // Get the pivot element from the middle of the list
		int middle = o + (N-o)/2;
		DataRow pivot = dt.get(middle);
        //int pivot = numbers[low + (high-low)/2];

        // Divide into two lists
        while (i <= j) {
            // If the current value from the left list is smaller than the pivot
            // element then get the next element from the left list
        	System.out.println(i);
        	DataRow dri = dt.get(i);
        	Integer m = (Integer)pivot.CompareTo(dri,key);
            while (m>0) {
                i++;
                dri = dt.get(i);
            	m = (Integer)pivot.CompareTo(dri,key);
                
            }
            // If the current value from the right list is larger than the pivot
            // element then get the next element from the right list
            DataRow drj = dt.get(j);
            m = (Integer)pivot.CompareTo(drj,key);
            while (m<0) {
                j--;
                drj = dt.get(j);
                m = (Integer)pivot.CompareTo(drj,key);
            }

            // If we have found a value in the left list which is larger than
            // the pivot element and if we have found a value in the right list
            // which is smaller than the pivot element then we exchange the
            // values.
            // As we are done we can increase i and j
            if (i <= j) {
            	swap(i, j);
                i++;
                j--;
            }
        }
        // Recursion
        if (o < j)
        	orderby(o, j,key);
        if (i <N)
        	orderby(i, N,key);
	}
	/*public void orderby(int o,int N,String key) throws Exception{
		if(o<N){
			if(o==3 && N==4){
				int k=0;
			}
			int middle = o + (N - o) / 2;
			orderby(o,middle,key);
			orderby(middle+1,N,key);
			merge(o,N,key);
		}
	}*/
	
	public void swap(int i,int j) throws Exception{
		
		DataRow dri = dt.get(i);
		DataRow drj = dt.get(j);
		dt.set(i, dt.get(j));
		dt.set(j, dri);
		dri = dt.get(i);
		drj = dt.get(j);
		
		
	}
	public void merge(int o,int N,String key) throws Exception{
		int middle = o + (N - o) / 2;
		middle++;
		int i=o;
		int j=middle;
		String v="";
		
		while(i<=middle && j<=N){
			DataRow dri = dt.get(i);
			DataRow drj = dt.get(j);
			String ndb1 = dri.get_String(key);
			String ndb2 = drj.get_String(key);
			if(ndb1.compareTo("01004")==0 && ndb2.compareTo("01003")==0){
				System.out.println("entro");
			}
			
			Integer m = (Integer)drj.CompareTo(dri,key);
			if(m.intValue()<0){
				swap(i,j);
				if(ndb1.compareTo("01003")==0 || ndb2.compareTo("01003")==0){
					writeJson("C:/prueba2.json");
				}
				j++;
			}
			else{
				i++;
			}
		}
		if(j>N){
			j--;
			while(i<=middle){
				DataRow dri = dt.get(i);
				DataRow drj = dt.get(j);
				String ndb1 = dri.get_String(key);
				String ndb2 = drj.get_String(key);
				Integer m = (Integer)drj.CompareTo(dri,key);
				if(m.intValue()<0){
					swap(i,j);
				}
				
			}
			
		}
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
        		if(i==2){
        			System.out.println("entro aqui");
        		}
        		iguales = iguales && dt.get(i).equals(table.row(i));
        		if(!iguales){
        			System.out.println(i);
        		}
        		
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
