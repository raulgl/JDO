import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
	public final void testDataTable() {
		fail("Not yet implemented"); // TODO
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

}
