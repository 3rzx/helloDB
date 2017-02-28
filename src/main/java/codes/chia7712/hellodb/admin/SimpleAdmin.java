package codes.chia7712.hellodb.admin;

import codes.chia7712.hellodb.Table;
import codes.chia7712.hellodb.data.BytesUtil;
import codes.chia7712.hellodb.data.Cell;
import codes.chia7712.hellodb.data.CellComparator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

class SimpleAdmin implements Admin {

  private final ConcurrentMap<String, Table> tables = new ConcurrentSkipListMap<>();
  private Properties p;
  private static String userName;
  
  SimpleAdmin(Properties prop) throws Exception {
	  this.p = prop;
	  //取出userName or 設成Default
	  userName = p.getProperty("userName","Default");
	  
	  //循序取出key 由userNameFile建立tables
	  File file = new File(userName);
	  file.createNewFile();
	  FileInputStream f = new FileInputStream(file);
	  InputStreamReader s = new InputStreamReader(f, "UTF-8");
	  BufferedReader br = new BufferedReader(s);
	  String line;
	  while((line = br.readLine()) !=null){
		  tables.computeIfAbsent(line,SimpleTable::new);
	  }
	  s.close();
  }

  @Override
  public void createTable(String name) throws IOException {
    if (tables.containsKey(name)) {
      throw new IOException(name + " exists");
    }
    tables.computeIfAbsent(name,SimpleTable::new);
  }

  @Override
  public boolean tableExist(String name) throws IOException {
    return tables.containsKey(name);
  }

  @Override
  public void deleteTable(String name) throws IOException {
    if (tables.remove(name) == null) {
      throw new IOException(name + " not found");
    }
    File file = new File(userName+name);
    if(!file.delete()){
    	throw new IOException(name + " not found");
    }
  }

  @Override
  public Table openTable(String name) throws IOException {
    Table t = tables.get(name);
    if (t == null) {
      throw new IOException(name + " not found");
    }
    return t;
  }

  @Override
  public List<String> listTables() throws IOException {
    return tables.keySet().stream().collect(Collectors.toList());
  }

  @Override
  public void close() throws IOException {
	 
	 File file = new File(userName);
     FileOutputStream f = new FileOutputStream(file);
     OutputStreamWriter s = new OutputStreamWriter(f);
     String WriteToFile="";
     for (Map.Entry<String, Table> entry : tables.entrySet()){
		WriteToFile = entry.getValue().getName() + "\n";
		s.write(WriteToFile);
		if(entry.getValue().isOpen()){
			this.openTable(entry.getValue().getName()).close();
		}
	 }
     s.close();
	    
  }

  private static class SimpleTable implements Table {

	  private static final CellComparator CELL_COMPARATOR = new CellComparator();
	  private final ConcurrentNavigableMap<Cell, Cell> data = new ConcurrentSkipListMap<>(CELL_COMPARATOR);
	  private final String name;
	  private boolean isOpen = false;

    SimpleTable(final String name) {
      this.name = name;
      File file = new File(userName + name);
      FileInputStream f;
      InputStreamReader s;
      BufferedReader br; 
  	  try {
  		  file.createNewFile();
  		  f = new FileInputStream(file);
  		  s = new InputStreamReader(f,"UTF-8");
  		  br = new BufferedReader(s);
	  	  String line;
	  	  while((line = br.readLine()) != null){
	  		  String content[] = line.split("\\|");
	  		  Cell c = Cell.createCell(BytesUtil.toBytes(content[0]),Integer.parseInt(content[1]),Integer.parseInt(content[2]),
	  				BytesUtil.toBytes(content[3]),Integer.parseInt(content[4]),Integer.parseInt(content[5]),
	  				BytesUtil.toBytes(content[6]),Integer.parseInt(content[7]),Integer.parseInt(content[8]));
	  		  data.put(c,c);
	  	  }
  		  s.close();
  		  
  	  } catch (FileNotFoundException e) {
		e.printStackTrace();
  	  } catch (IOException e) {
		e.printStackTrace();
  	  }
    }

    @Override
    public boolean insert(Cell cell) throws IOException {
    	//old value will be replace
    	isOpen = true;
    	return data.put(cell, cell) != null;
        //insert failed will return null
    }

    @Override
    public void delete(byte[] row) throws IOException {
      isOpen = true;
      Cell rowOnlyCell = Cell.createRowOnly(row);
      for (Map.Entry<Cell, Cell> entry : data.tailMap(rowOnlyCell).entrySet()) {
        if (CellComparator.compareRow(entry.getKey(), rowOnlyCell) != 0) {
          return;
        } else {
          data.remove(entry.getKey());
        }
      }
    }

    @Override
    public Iterator<Cell> get(byte[] row) throws IOException {
      Cell rowOnlyCell = Cell.createRowOnly(row);
      List<Cell> rval = new ArrayList<>();
      for (Map.Entry<Cell, Cell> entry : data.tailMap(rowOnlyCell).entrySet()) {
        if (CellComparator.compareRow(entry.getKey(), rowOnlyCell) != 0) {
          break;
        } else {
          rval.add(entry.getValue());
        }
      }
      return rval.iterator();
    }

    @Override
    public Optional<Cell> get(byte[] row, byte[] column) throws IOException {
      return Optional.ofNullable(data.get(Cell.createRowColumnOnly(row, column)));
    }

    @Override
    public boolean delete(byte[] row, byte[] column) throws IOException {
      return data.remove(Cell.createRowColumnOnly(row, column)) != null;
    }

    @Override
    public boolean insertIfAbsent(Cell cell) throws IOException {
        //insert only if the key is not already associated
    	isOpen = true;
    	return data.putIfAbsent(cell, cell) == null;
    	//return true only if there isn't a key 'cell' && insert failed
    }

    @Override
    public void close() throws IOException {
      // 將userName + name寫回去
    	File file = new File(userName + name);
        FileOutputStream f = new FileOutputStream(file);
        OutputStreamWriter s = new OutputStreamWriter(f);
        String WriteToFile="";
		for (Map.Entry<Cell, Cell> entry : data.entrySet()){
			WriteToFile = Parser.CellToFile(entry.getValue()) + "\n";
			s.write(WriteToFile);
		}
        s.close();
    }

    @Override
    public String getName() {
      return name;
    }
    
    public boolean isOpen(){
    	return isOpen;
    }

  }

  private static class Parser{
	  public static String CellToFile(Cell cell){
		String WriteToFile="";
		try {
			WriteToFile += new String(cell.getRowArray(), "UTF-8");
			WriteToFile += "|";
			WriteToFile += cell.getRowOffset();
			WriteToFile += "|";
			WriteToFile += cell.getRowLength();
			WriteToFile += "|";
			WriteToFile += new String(cell.getColumnArray(),"UTF-8");
			WriteToFile += "|";
			WriteToFile += cell.getColumnOffset();
			WriteToFile += "|";
			WriteToFile += cell.getColumnLength();
			WriteToFile += "|";
			WriteToFile += new String(cell.getValueArray(),"UTF-8");
			WriteToFile += "|";
		  	WriteToFile += cell.getValueOffset();
		  	WriteToFile += "|";
		  	WriteToFile += cell.getValueOffset();
		  	WriteToFile += "|";
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		  return WriteToFile; 
	  }
  }
  
}
