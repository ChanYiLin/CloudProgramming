//scoreboard
package SearchEngine;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.io.File;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Scoreboard implements Writable {

	private ArrayList<TermFileInfo> TermFileInfos;

	public Scoreboard(){
		this.TermFileInfos  = new ArrayList<TermFileInfo>();
	}
	
	public Scoreboard(ArrayList<TermFileInfo> TermFileInfos) {
		this.TermFileInfos  = TermFileInfos;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		
		// Write arrayList TermFileInfos
		int Len = TermFileInfos.size();
		out.writeInt(Len);
		TermFileInfo TermFileInfo = new TermFileInfo();
		for (int i = 0; i < Len; i++) {
			TermFileInfo = TermFileInfos.get(i);
			
			// Write fileName
			Text.writeString(out, TermFileInfo.getFileName());
			
			// Write Score
			out.writeDouble(TermFileInfo.getScore());
			
			// Write offset
			int len = TermFileInfo.getOffset().size();
			out.writeInt(len);
			for (int j = 0; j < len; j++) {
				out.writeLong(TermFileInfo.getOffset().get(j));
			}
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		// Read docFrequency
		//docFreq = in.readInt();
				
		
		// Read arrayList TermFileInfos
		TermFileInfos.clear();
		int Len = in.readInt();
		for (int i = 0; i < Len; i++) {
					
			// Read fileName
			String fileName = Text.readString(in);
					
			// Read score
			Double score = in.readDouble();
			
			// Read offset
			int len = in.readInt();
			ArrayList<Long> arr = new ArrayList<Long>();
			for (int j = 0; j < len; j++) {
				arr.add(in.readLong());
			}
			TermFileInfo TermFileInfo = new TermFileInfo(fileName, score, arr);
			TermFileInfos.add(TermFileInfo);
		}
		
	}
	
	@Override
	public String toString(){

		TermFileInfo TermFileInfo = TermFileInfos.get(0);
		String str = ";";
		str = str + TermFileInfo.getFileName() + " ";
		str = str + TermFileInfo.getScore().toString() + " ";
		
		int len = TermFileInfo.getOffset().size();
		str = str.concat("[" + TermFileInfo.getOffset().get(0).toString());
		for (int j = 1; j < len; j++) {
			str = str.concat("," + TermFileInfo.getOffset().get(j).toString());
		}
		str = str.concat("]");	
		
		int Len = TermFileInfos.size();
		for (int i = 1; i < Len; i++) {
			TermFileInfo = TermFileInfos.get(i);
			str = str + ";";
			str = str + TermFileInfo.getFileName() + " ";
			str = str + TermFileInfo.getScore().toString() + " ";
			
			len = TermFileInfo.getOffset().size();
			str = str.concat("[" + TermFileInfo.getOffset().get(0).toString());
			for (int j = 1; j < len; j++) {
				str = str.concat("," + TermFileInfo.getOffset().get(j).toString());
			}
			str = str.concat("]");	
		}

		return str;
	}
	
	public ArrayList<TermFileInfo> getTermFileInfos(){
		return TermFileInfos;
	}

}