import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class DataTableTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public final void testDataTable() throws SQLException {
		Connection conexion = DriverManager.getConnection ("jdbc:mysql://localhost/farmacia2017","root", "patata");
		Statement s = conexion.createStatement();
		ResultSet rs = s.executeQuery ("select * from aglyconas");
		DataTable dt = new DataTable();
		dt.fill(rs);
		DataTable dt2 = new DataTable(dt);
		Set<String> cols = dt2.columnas;
		assertEquals(cols.contains("NDB_No"),true);
		assertEquals(cols.contains("Nutr_No"),true);
		assertEquals(cols.contains("valor"),true);
		assertEquals(cols.contains("valo"),false);
		
		
	}

	@Test
	public final void testRow() throws SQLException {
		Connection conexion = DriverManager.getConnection ("jdbc:mysql://localhost/farmacia2017","root", "patata");
		Statement s = conexion.createStatement();
		ResultSet rs = s.executeQuery ("select * from aglyconas");
		DataTable dt = new DataTable();
		dt.fill(rs);
		assertEquals(dt.row(1).get_double("valor"),0.339,0.001);
	}

	@Test
	public final void testFill() throws SQLException {
		Connection conexion = DriverManager.getConnection ("jdbc:mysql://localhost/farmacia2017","root", "patata");
		Statement s = conexion.createStatement();
		ResultSet rs = s.executeQuery ("select * from ffqs");
		DataTable dt = new DataTable();
		dt.fill(rs);
		assertEquals(dt.row(0).get_String("ffqs"),"ffq_french");
		
	}
	@Test
	public final void writeJson() throws Exception {
		Connection conexion = DriverManager.getConnection ("jdbc:mysql://localhost/farmacia2017","root", "patata");
		Statement s = conexion.createStatement();
		ResultSet rs = s.executeQuery ("select * from aglyconas");
		DataTable dt = new DataTable();
		dt.fill(rs);
		dt.writeJson("C:/prueba.json");
		DataTable dt2 = new DataTable("C:/prueba.json");
		assertEquals(dt.RowCount(),dt2.RowCount());
		assertEquals(dt.ColumCount(),dt2.ColumCount());
		assertEquals(dt,dt2);
		
		
	}
	@Test
	public final void NewRow() throws Exception {
		Connection conexion = DriverManager.getConnection ("jdbc:mysql://localhost/farmacia2017","root", "patata");
		Statement s = conexion.createStatement();
		ResultSet rs = s.executeQuery ("select * from aglyconas");
		DataTable dt = new DataTable();
		dt.fill(rs);
		DataRow row = dt.NewRow();
		row.Add(0, "TOTAL");
		row.Add(1, dt.RowCount());
		row.Add(2, "FILAS");
		assertEquals(row.get_String(0),row.get_String("Nutr_No"));
		assertEquals(row.get_int(1),row.get_int("valor"));
		assertEquals(row.get_String(2),row.get_String("NDB_No"));
		dt.addRow(row);
		dt.writeJson("C:/prueba.json");	
		
	}
	@Test
	public final void Copy() throws Exception {
		Connection conexion = DriverManager.getConnection ("jdbc:mysql://localhost/farmacia2017","root", "patata");
		Statement s = conexion.createStatement();
		ResultSet rs = s.executeQuery ("select * from aglyconas");
		DataTable dt = new DataTable();
		dt.fill(rs);
		DataTable dt2= dt.Copy();
		assertEquals(dt2.equals(dt),true);
		
	}
	
	
	

}
