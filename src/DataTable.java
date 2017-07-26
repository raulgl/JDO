import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
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
		columnas = patron.keys();
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
        for ( int i = 0 ; i < dt.size() ; i++ ) {
			iguales = iguales && dt.get(i).equals(table.row(i));
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
