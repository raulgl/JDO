import static org.junit.Assert.*;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.Date;
import java.sql.Time;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

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
		DataRow dr = new DataRow();
		dr.Add("key1", new Double(100.5));
		dr.Add("key3", new Integer(100));
		dr.Add("key2", "valor");
		dr.Add("key4", Date.valueOf("2017-07-05"));
		dr.Add("key5", Time.valueOf("12:02:00"));
		
		JsonObjectBuilder jo = Json.createObjectBuilder();
		try {
			dr.writeJson(jo,1);
			JsonObject jso = jo.build();
			JsonArray array = jso.getJsonArray("Row1");
			DataRow dr2 = new DataRow(array);
			assertEquals(dr2.get_double("key1"),dr.get_double("key1"),0.1);
			assertEquals(dr2.get_String("key2"),dr.get_String("key2"));
			assertEquals(dr2.get_int("key3"),dr.get_int("key3"));
			assertEquals(dr2.get_date("key4"),dr.get_date("key4"));
			assertEquals(dr2.get_time("key5"),dr.get_time("key5"));
			assertEquals(dr,dr2);
			
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
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
	@Test
	public final void writeJson() throws ClassNotFoundException, IOException {
		DataRow dr = new DataRow();
		dr.Add("key1", new Double(100.5));
		dr.Add("key3", new Integer(100));
		dr.Add("key2", "valor");
		JsonObjectBuilder jo = Json.createObjectBuilder();
		dr.writeJson(jo,1);
		JsonObject jso = jo.build();
		jso.toString();
		assertEquals(jo.build().toString(),"\"{Row\":[{\"key\":\"Key\",\"Tipo\":\"Double\",\"Valor\":\"100.5\"}]}");
	}
	@Test
	public final void Copy() throws ClassNotFoundException, IOException {
		DataRow dr = new DataRow();
		dr.Add("key1", new Double(100.5));
		dr.Add("key3", new Integer(100));
		dr.Add("key2", "valor");
		dr.Add("key4", Date.valueOf("2017-07-05"));
		dr.Add("key5", Time.valueOf("12:02:00"));
		DataRow dr2 = dr.Copy();
		assertEquals(dr2.equals(dr),true);
		
		
		
		
	}

}
