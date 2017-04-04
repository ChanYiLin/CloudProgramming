package Retrieval;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.io.File;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
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
		
		// Write docFreq
		out.writeInt(docFreq);
		
		
		// Write arrayList WordInfos
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
		// Output format:
		// docFrequency fileName termFrequency [offset1,offset2,...]
		
		String str = Integer.toString(docFreq) + " ";
		
		
		WordInfo WordInfo = WordInfos.get(0);
		str = str + WordInfo.getFileName() + " ";
		str = str + WordInfo.getTermFreq().toString() + " ";
		
		int len = WordInfo.getOffset().size();
		str = str.concat("[" + WordInfo.getOffset().get(0).toString());
		for (int j = 1; j < len; j++) {
			str = str.concat("," + WordInfo.getOffset().get(j).toString());
		}
		str = str.concat("]");	
		
		int Len = WordInfos.size();
		for (int i = 1; i < Len; i++) {
			WordInfo = WordInfos.get(i);
			str = str + ";";
			str = str + WordInfo.getFileName() + " ";
			str = str + WordInfo.getTermFreq().toString() + " ";
			
			len = WordInfo.getOffset().size();
			str = str.concat("[" + WordInfo.getOffset().get(0).toString());
			for (int j = 1; j < len; j++) {
				str = str.concat("," + WordInfo.getOffset().get(j).toString());
			}
			str = str.concat("]");	
		}

		return str;
	}
	
	public int getDocFreq() {
		return docFreq;
	}
	
	public ArrayList<WordInfo> getWordInfos(){
		return WordInfos;
	}
}