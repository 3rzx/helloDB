package codes.chia7712.hellodb.admin;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.Properties;

import codes.chia7712.hellodb.Table;
import codes.chia7712.hellodb.data.Cell;


public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub	
		try {
			Properties props = new Properties();
			props.setProperty(Admin.ADMIN_IMPL, Admin.DEFAULT_ADMIN_IMPL);
			props.setProperty("userName", "User1");
			SimpleAdmin user = new SimpleAdmin(props);
			
			System.out.println("Open Table1:(If doesn't exist create it instead)");
			if(!user.tableExist("Table1")){
				user.createTable("Table1");
			}
			Table nowT = user.openTable("Table1");
			
			Cell c1 = Cell.createCell("firstR".getBytes(), "firstC".getBytes(), "1R1C".getBytes());
			Cell c2 = Cell.createCell("secondR".getBytes(),"firstC".getBytes(), "2R1C".getBytes());
			Cell c3 = Cell.createCell("firstR".getBytes(),"secondC".getBytes(), "1R2C".getBytes());
			Cell c4 = Cell.createCell("secondR".getBytes(), "secondC".getBytes(), "2R2C".getBytes());
			System.out.println("Insert 4 cell in table.\n");
			nowT.insert(c1);
			nowT.insert(c2);
			nowT.insert(c3);
			nowT.insert(c4);

			System.out.println("Get firstRow and firstCol cell:");
			Optional<Cell> c = nowT.get("firstR".getBytes(), "firstC".getBytes());
			if(c.isPresent())
				System.out.println(new String(c.get().getValueArray(),"UTF-8"));
//			
			System.out.println("\nfirst Row iterator.");
			Iterator<Cell> iter = nowT.get("firstR".getBytes());
			while(iter.hasNext())
				System.out.println(new String(iter.next().getValueArray()));
			
			System.out.println("\nDelete firstRow and secondCol cell");
			nowT.delete("firstR".getBytes(),"secondC".getBytes());
			
			System.out.println("Get firstRow and secondCol cell:(it should have no value)");
			c = nowT.get("firstR".getBytes(), "secondC".getBytes());
			if(c.isPresent())
				System.out.println(new String(c.get().getValueArray(),"UTF-8"));
			else
				System.out.println("no value");
			
			System.out.println("\nClose this admin");
			user.close();

			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
