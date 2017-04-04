package InvertedIndex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.io.File;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

//import part4.Retrieval;

public class TableValue implements Writable {
	static String   INPUT_DIR  = "./input";
	private int						docFreq;
	private ArrayList<WordInfo>   WordInfos;

	public TableValue(){
		this.docFreq 	  = 0;
		this.WordInfos  = new ArrayList<WordInfo>();
	}
	
	public TableValue(int docFreq, ArrayList<WordInfo> WordInfos) {
		this.docFreq	  = docFreq;
		this.WordInfos  = WordInfos;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeInt(docFreq);
	
		int Len = WordInfos.size();
		out.writeInt(Len);
		WordInfo WordInfo = new WordInfo();
		for (int i = 0; i < Len; i++) {
			WordInfo = WordInfos.get(i);
			
			// Write fileName
			Text.writeString(out, WordInfo.getFileName());
			
			// Write termFreq
			out.writeDouble(WordInfo.getTermFreq());
			
			// Write offset
			int len = WordInfo.getOffset().size();
			out.writeInt(len);
			for (int j = 0; j < len; j++) {
				out.writeLong(WordInfo.getOffset().get(j));
			}
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		// Read docFrequency
		docFreq = in.readInt();
				
		
		// Read arrayList WordInfos
		WordInfos.clear();
		int Len = in.readInt();
		for (int i = 0; i < Len; i++) {
					
			// Read fileName
			String fileName = Text.readString(in);
					
			// Read termFreq
			Double termFreq = in.readDouble();
			
			// Read offset
			int len = in.readInt();
			ArrayList<Long> arr = new ArrayList<Long>();
			for (int j = 0; j < len; j++) {
				arr.add(in.readLong());
			}
			WordInfo WordInfo = new WordInfo(fileName, termFreq, arr);
			WordInfos.add(WordInfo);
		}
		
	}
	
	@Override
	public String toString(){
		
		String str = Integer.toString(docFreq) + "<maindiv>";
		StringBuffer tmp = new StringBuffer(str);
		
		WordInfo WordInfo = WordInfos.get(0);
		tmp.append(WordInfo.getFileName());
		tmp.append("<div>");
		tmp.append(WordInfo.getTermFreq().toString());
		tmp.append("<div>");
		/*str = str + WordInfo.getFileName() + " ";
		str = str + WordInfo.getTermFreq().toString() + " ";*/
		int len = WordInfo.getOffset().size();


		tmp.append("[");
		tmp.append(WordInfo.getOffset().get(0).toString());
		//str = str.concat("[" + WordInfo.getOffset().get(0).toString());
		for (int j = 1; j < len; j++) {
			tmp.append(",");
			tmp.append(WordInfo.getOffset().get(j).toString());
			//str = str.concat("," + WordInfo.getOffset().get(j).toString());
		}
		tmp.append("]");
		//str = str.concat("]");	
		
		int Len = WordInfos.size();
		for (int i = 1; i < Len; i++) {
			WordInfo = WordInfos.get(i);
			tmp.append("<maindiv>");
			tmp.append(WordInfo.getFileName());
			tmp.append("<div>");
			tmp.append(WordInfo.getTermFreq().toString());
			tmp.append("<div>");
			//str = str + ";";
			//str = str + WordInfo.getFileName() + " ";
			//str = str + WordInfo.getTermFreq().toString() + " ";
			
			len = WordInfo.getOffset().size();
			tmp.append("[");
			tmp.append(WordInfo.getOffset().get(0).toString());
			//str = str.concat("[" + WordInfo.getOffset().get(0).toString());
			for (int j = 1; j < len; j++) {
				tmp.append(",");
				tmp.append(WordInfo.getOffset().get(j).toString());
				//str = str.concat("," + WordInfo.getOffset().get(j).toString());
			}
			tmp.append("]");
			//str = str.concat("]");	
		}
		str = tmp.toString();
		return str;
	}
	
	public int getDocFreq() {
		return docFreq;
	}
	
	public ArrayList<WordInfo> getWordInfos(){
		return WordInfos;
	}
}