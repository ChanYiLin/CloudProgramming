package InvertedIndex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class I2Value implements Writable {
	private ArrayList<Long> offset;
	private Double 			termFrequency;

	public I2Value(){
		this.offset 	   = new ArrayList<Long>();
		this.termFrequency = new Double(0);
	}

	public I2Value(ArrayList<Long> offset, Double termFrequency) {
		this.offset 	   = offset;
		this.termFrequency = termFrequency;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		// Write arrayList offset
		int len = offset.size();
		out.writeInt(len);
		for (int i = 0; i < len; i++) {
			out.writeLong(offset.get(i));
		}
		
		// Write termFrequency
		out.writeDouble(termFrequency);
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		// Read arrayList offset
		offset.clear();
		int i = in.readInt();
		for (; i > 0; i--) {
			offset.add(in.readLong());
		}
		
		// Read termFrequency
		termFrequency = in.readDouble();
	}
	
	@Override
	public String toString(){
		// Output format:
		// termFrequency [offset1,offset2,...]

				
		String str = termFrequency.toString() + "&gt;";
		StringBuffer tmp = new StringBuffer(str);

				
		int len = offset.size();
		tmp.append("[");
		tmp.append(offset.get(0).toString());
		
		for (int i = 1; i < len; i++) {
			tmp.append(",");
			tmp.append(offset.get(i).toString());
		}
		tmp.append("]");
		str = tmp.toString();

		return str;
	}
	
	
	public ArrayList<Long> getOffset(){
		return offset;
	}
	
	public Double getTermFrequency() {
		return termFrequency;
	}
}