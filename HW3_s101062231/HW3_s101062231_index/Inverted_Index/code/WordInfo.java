//WordInfo
package InvertedIndex;
import java.util.ArrayList;

public class WordInfo{
	private String fileName;
	private Double termFrequency;
	private ArrayList<Long> offset;

	public WordInfo() {
		this.fileName = "";
		this.termFrequency = (Double)0.0;
		this.offset   = new ArrayList<Long>();
	}
	
	public WordInfo(String fileName, Double termFrequency, ArrayList<Long> offset) {
		this.fileName = fileName;
		this.termFrequency = termFrequency;
		this.offset   = offset;
	}
	
	public String getFileName(){
		return fileName;
	}
	
	public Double getTermFreq(){
		return termFrequency;
	}
	
	public ArrayList<Long> getOffset(){
		return offset;
	}
	
	public void setTermFreq(Double termFrequency) {
		this.termFrequency = termFrequency;
	}
	
	public void setOffset(ArrayList<Long> offset) {
		this.offset = offset;
	}
	
	public void appendOffset(ArrayList<Long> offset) {
		this.offset.addAll(offset);
	}

}