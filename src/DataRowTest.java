import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class DataRowTest {

	@Before
	public void setUp() throws Exception {
		
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public final void testDataRow() {
		
		
	}

	@Test
	public final void testAdd() {
		DataRow dr = new DataRow();
		dr.Add("key", "valor");
		String resultado = dr.get_String("key");
		assertEquals(resultado,"valor");
	}

	@Test
	public final void testGet_String() {
		DataRow dr = new DataRow();
		dr.Add("key", "valor");
		String resultado = dr.get_String("key");
		assertEquals(resultado,"valor");
	}

	@Test
	public final void testGet_int() {
		DataRow dr = new DataRow();
		dr.Add("key", new Integer(100));
		int resultado = dr.get_int("key");
		assertEquals(resultado,100);
	}
	@Test
	public final void testGet_double() {
		DataRow dr = new DataRow();
		dr.Add("key", new Double(100.5));
		double resultado = dr.get_double("key");
		assertEquals(resultado,100.5,0.1);
	}

}
