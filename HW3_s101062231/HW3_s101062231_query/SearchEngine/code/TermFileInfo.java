//TermFileInfo.java

//file1 1.XXXX [a,b,c,d];
//file2 2.XXXXXX [a,b];
//file4 3.XXXXX [a];
package SearchEngine;

import java.util.ArrayList;

public class TermFileInfo{
	private String fileName;
	//private Double termFrequency;
	private Double score;
	private ArrayList<Long> offset;

	public TermFileInfo() {
		this.fileName = "";
		this.score = (Double)0.0;
		this.offset   = new ArrayList<Long>();
	}
	
	public TermFileInfo(String fileName, Double score, ArrayList<Long> offset) {
		this.fileName = fileName;
		this.score = score;
		this.offset   = offset;
	}
	
	public String getFileName(){
		return fileName;
	}
	
	public Double getScore(){
		return score;
	}
	
	public ArrayList<Long> getOffset(){
		return offset;
	}
	
	public void setScore(Double score) {
		this.score = score;
	}
	
	public void setOffset(ArrayList<Long> offset) {
		this.offset = offset;
	}
	
	public void appendOffset(ArrayList<Long> offset) {
		this.offset.addAll(offset);
	}

}