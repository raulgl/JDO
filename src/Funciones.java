
public class Funciones {
	public static boolean buscar(String[] list,String llave){
		boolean encontrado=false;
		int i=0;
		while(i<list.length && !encontrado){
			encontrado = llave.compareTo(list[i].toUpperCase())==0;
			i++;
		}
		return encontrado;
	}

}
