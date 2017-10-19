import static org.junit.Assert.*;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.Date;
import java.sql.Time;
import java.util.LinkedHashSet;

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
	public final void testDataRow() throws Exception {
		DataTable dt = new DataTable();
		dt.add_columna("key1");
		dt.add_columna("key2");
		dt.add_columna("key3");
		dt.add_columna("key4");
		dt.add_columna("key5");
		DataRow dr = dt.NewRow();
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
			DataRow dr2 = new DataRow(array,dt.columnas);
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
	public final void testAdd() throws Exception {
		DataTable dt = new DataTable();
		dt.add_columna("key");
		DataRow dr = dt.NewRow();
		dr.Add("key", "valor");
		String resultado = dr.get_String("key");
		assertEquals(resultado,"valor");
	}

	@Test
	public final void testGet_String() throws Exception {
		DataTable dt = new DataTable();
		dt.add_columna("key");
		DataRow dr = dt.NewRow();
		dr.Add("key", "valor");
		String resultado = dr.get_String("key");
		assertEquals(resultado,"valor");
	}

	@Test
	public final void testGet_int() throws Exception {
		DataTable dt = new DataTable();
		dt.add_columna("key");
		DataRow dr = dt.NewRow();
		dr.Add("key", new Integer(100));
		int resultado = dr.get_int("key");
		assertEquals(resultado,100);
	}
	@Test
	public final void testGet_double() throws Exception {
		DataTable dt = new DataTable();
		dt.add_columna("key");
		DataRow dr = dt.NewRow();
		dr.Add("key", new Double(100.5));
		double resultado = dr.get_double("key");
		assertEquals(resultado,100.5,0.1);
	}
	@Test
	/*public final void writeJson() throws ClassNotFoundException, IOException {
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
	@Test*/
	public final void Copy() throws Exception {
		DataTable dt = new DataTable();
		dt.add_columna("key1");
		dt.add_columna("key2");
		dt.add_columna("key3");
		dt.add_columna("key4");
		dt.add_columna("key5");
		DataRow dr = dt.NewRow();
		dr.Add("key1", new Double(100.5));
		dr.Add("key3", new Integer(100));
		dr.Add("key2", "valor");
		dr.Add("key4", Date.valueOf("2017-07-05"));
		dr.Add("key5", Time.valueOf("12:02:00"));
		DataRow dr2 = dr.Copy();
		assertEquals(dr2.equals(dr),true);
		
		
		
		
	}
	@Test
	public final void CompareTo() throws Exception{
		DataTable dt = new DataTable();
		dt.add_columna("id");
		dt.add_columna("no");
		dt.add_columna("valor");
		DataRow dr = dt.NewRow();
		dr.Add("id", new Integer(1));
		dr.Add("no", new Integer(2));
		dr.Add("valor", new Double(100.5));
		DataRow dr2 = dr.Copy();
		DataRow dr3 = dt.NewRow();
		dr3.Add("id", new Integer(1));
		dr3.Add("no", new Integer(1));
		dr3.Add("valor", new Double(100));
		String[] keys = {"id","no"};
		Integer m =(Integer) dr.CompareTo(dr2, keys);
		assertEquals(m.intValue(),0);
		m =(Integer) dr.CompareTo(dr3, keys);
		assertEquals(m.intValue(),1);
		
	}
	@Test
	public final void DataRow_array_string() throws Exception{
		DataTable dt = new DataTable();
		dt.add_columna("id");
		dt.add_columna("no");
		dt.add_columna("valor");
		DataRow dr = dt.NewRow();
		dr.Add("id", new Integer(1));
		dr.Add("no", new Integer(2));
		dr.Add("valor", new Double(100.5));
		dt = new DataTable();
		dt.add_columna("id");
		dt.add_columna("no");
		dt.add_columna("ingesta");
		DataRow dr3 = dt.NewRow();
		dr3.Add("id", new Integer(1));
		dr3.Add("no", new Integer(2));
		dr3.Add("ingesta", new Double(100));
		String[] key = {"id","no"};
		DataRow dr2 = new DataRow(dr,dr3,key);
		assertEquals(dr2.Count(),4);
		assertEquals(dr2.get_int("id").intValue(),1);
		assertEquals(dr2.get_int("no").intValue(),2);
		assertEquals(dr2.get_double("valor"),100.5,0.5);
		assertEquals(dr2.get_double("ingesta"),100,0.5);
		
	}

}
