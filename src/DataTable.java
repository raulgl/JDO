import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;


public class DataTable {
	ArrayList<DataRow> dt;
	public DataTable(){
		dt = new ArrayList();
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
	}
	
	
}
