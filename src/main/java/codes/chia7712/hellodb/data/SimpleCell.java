package codes.chia7712.hellodb.data;

import java.io.UnsupportedEncodingException;

class SimpleCell implements Cell {

  private final byte[] rowArray;
  private final int rowOffset;
  private final int rowLength;
  private final byte[] columnArray;
  private final int columnOffset;
  private final int columnLength;
  private final byte[] valueArray;
  private final int valueOffset;
  private final int valueLength;

  SimpleCell(final byte[] rowArray, final int rowOffset, final int rowLength,
          final byte[] columnArray, final int columnOffset, final int columnLength,
          final byte[] valueArray, final int valueOffset, final int valueLength) {
    this.rowArray = rowArray;
    this.rowOffset = rowOffset;
    this.rowLength = rowLength;
    this.columnArray = columnArray;
    this.columnOffset = columnOffset;
    this.columnLength = columnLength;
    this.valueArray = valueArray;
    this.valueOffset = valueOffset;
    this.valueLength = valueLength;
  }

  @Override
  public byte[] getRowArray() {
    return rowArray;
  }

  @Override
  public int getRowOffset() {
    return rowOffset;
  }

  @Override
  public int getRowLength() {
    return rowLength;
  }

  @Override
  public byte[] getColumnArray() {
    return columnArray;
  }

  @Override
  public int getColumnOffset() {
    return columnOffset;
  }

  @Override
  public int getColumnLength() {
    return columnLength;
  }

  @Override
  public byte[] getValueArray() {
    return valueArray;
  }

  @Override
  public int getValueOffset() {
    return valueOffset;
  }

  @Override
  public int getValueLength() {
    return valueLength;
  }
  
  public String toString(){
	  String WriteToFile="";
	  try {
		WriteToFile += new String(this.rowArray, "UTF-8");
		WriteToFile += "|";
		WriteToFile += this.rowOffset;
		WriteToFile += "|";
		WriteToFile += this.rowLength;
		WriteToFile += "|";
		WriteToFile += new String(this.columnArray, "UTF-8");
		WriteToFile += "|";
		WriteToFile += this.columnOffset;
		WriteToFile += "|";
		WriteToFile += this.columnLength;
		WriteToFile += "|";
		WriteToFile += new String(this.valueArray, "UTF-8");
		WriteToFile += "|";
	  	WriteToFile += this.valueOffset;
	  	WriteToFile += "|";
	  	WriteToFile += this.columnLength;
	  	WriteToFile += "|";
	  } catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
	  }
	  return WriteToFile; 
  }
  

}
