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
	public final void testDataTable() throws Exception {
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
	public final void testRow() throws Exception {
		Connection conexion = DriverManager.getConnection ("jdbc:mysql://localhost/farmacia2017","root", "patata");
		Statement s = conexion.createStatement();
		ResultSet rs = s.executeQuery ("select * from aglyconas");
		DataTable dt = new DataTable();
		dt.fill(rs);
	    java.math.BigDecimal bg = new java.math.BigDecimal(dt.row(1).get("valor").toString());
		assertEquals(bg,0.339);
	}

	@Test
	public final void testFill() throws Exception {
		Connection conexion = DriverManager.getConnection ("jdbc:mysql://localhost/farmacia2017","root", "patata");
		Statement s = conexion.createStatement();
		ResultSet rs = s.executeQuery ("select * from ffqs");
		DataTable dt = new DataTable();
		dt.fill(rs);
		assertEquals(dt.row(0).get_String("ffqs"),"ffq_french");
		
	}
	@Test
	public final void writeJson() throws Exception {
		Connection conexion = DriverManager.getConnection ("jdbc:mysql://localhost/prostr","root", "patata");
		Statement s = conexion.createStatement();
		ResultSet rs = s.executeQuery ("select * from pfpagad limit 100");
		DataTable dt = new DataTable();
		dt.fill(rs);
		dt.writeJson("C:/prueba.json");
		DataTable dt2 = new DataTable("C:/prueba.json");
		assertEquals(dt.RowCount(),dt2.RowCount());
		assertEquals(dt.ColumCount(),dt2.ColumCount());
		assertEquals(dt2.equals(dt),true);
		
		
		
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
		assertEquals(row.get_int(1),row.get_int("Nutr_No"));
		assertEquals(row.get_String(2),row.get_String("valor"));
		assertEquals(row.get_String(0),row.get_String("NDB_No"));
		dt.addRow(row);
		
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
	@Test
	public final void Orderby() throws Exception {
		Connection conexion = DriverManager.getConnection ("jdbc:mysql://localhost/farmacia2017","root", "patata");
		Statement s = conexion.createStatement();
		ResultSet rs = s.executeQuery ("SELECT * FROM fdb_expro01 order by NDB_Name");
		DataTable dt = new DataTable();
		dt.fill(rs);		
		DataTable dt2= dt.Copy();
		dt2.orderby("NDB_No");		
		assertEquals(dt.equals(dt2),false);
		rs = s.executeQuery ("SELECT * FROM fdb_expro01 order by NDB_No");
		DataTable dt3 = new DataTable();
		dt3.fill(rs);		
		assertEquals(dt3.equals(dt2),true);	
		
	}
	@Test
	public final void join() throws Exception {
		Connection conexion = DriverManager.getConnection ("jdbc:mysql://localhost/farmacia2017","root", "patata");
		Statement s = conexion.createStatement();
		ResultSet rs = s.executeQuery ("SELECT * FROM fdb_expro01");
		DataTable dt = new DataTable();
		dt.fill(rs);
		rs = s.executeQuery ("SELECT * FROM flav_dat");
		DataTable dt2 = new DataTable();
		dt2.fill(rs);
		
		DataTable result = dt.join(dt2, "NDB_No");
		String[] keys = {"NDB_No","Nutr_No"};
		result.orderby(keys);
		rs = s.executeQuery ("SELECT flav_dat.*,NDB_Name,FdGrp_Cd FROM flav_dat,fdb_expro01 where flav_dat.NDB_No=fdb_expro01.NDB_No order by NDB_No,Nutr_No");
		DataTable dt3 = new DataTable();
		dt3.fill(rs);
		
		assertEquals(dt3.RowCount(),result.RowCount());
		assertEquals(dt3.equals(result),true);	
	}
	@Test
	public final void orderby() throws Exception{
		Connection conexion = DriverManager.getConnection ("jdbc:mysql://localhost/farmacia2017","root", "patata");
		Statement s = conexion.createStatement();
		ResultSet rs = s.executeQuery ("SELECT * FROM flav_dat order by valor");
		DataTable dt = new DataTable();
		dt.fill(rs);
		String[] keys = {"NDB_No","Nutr_No"};
		dt.orderby(keys);
		rs = s.executeQuery ("SELECT * FROM flav_dat order by NDB_No,Nutr_No");
		DataTable dt2 = new DataTable();
		dt2.fill(rs);
		assertEquals(dt.equals(dt2),true);
		
	}
	@Test
	public final void join_arrays() throws Exception {
		Connection conexion = DriverManager.getConnection ("jdbc:mysql://localhost/prostr","root", "patata");
		Statement s = conexion.createStatement();
		ResultSet rs = s.executeQuery ("SELECT ecfunc,ectipo as CCTIPO,eccicl as CCCICL FROM pfcfunc");
		DataTable dt = new DataTable();
		dt.fill(rs);
		rs = s.executeQuery ("SELECT * FROM pfccicl");
		DataTable dt2 = new DataTable();
		dt2.fill(rs);
		String[] keys = {"CCTIPO","CCCICL"};
		DataTable result = dt.join(dt2, keys);
		String[] orders = {"CCTIPO","CCCICL","ecfunc"};
		result.orderby(orders);
		result.writeCSV("C:/prueba0.csv");
		rs = s.executeQuery ("SELECT ecfunc,pfccicl.* FROM pfcfunc,pfccicl where ectipo=cctipo and eccicl=cccicl order by cctipo,cccicl,ecfunc");
		DataTable dt3 = new DataTable();
		dt3.fill(rs);
		dt3.writeCSV("C:/prueba.csv");
		assertEquals(dt3.RowCount(),result.RowCount());
		assertEquals(dt3.equals(result),true);
		
	}
	
	

}
