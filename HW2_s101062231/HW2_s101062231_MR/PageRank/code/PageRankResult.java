//PageRankResult.java
package PageRank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class PageRankResult implements Writable, WritableComparable<PageRankResult> {

	private String page;
	private double rank;

	public PageRankResult() {
	}

	public PageRankResult(String page, double rank) {
		this.page = page;
		this.rank = rank;
	}

	public String getName() {
		return page;
	}

	public double getRank() {
		return rank;
	}

	public int compareTo(PageRankResult target) {
		if (target.rank < rank)
			return -1;
		
		if (target.rank > rank)
			return 1; 
		
		return page.compareTo(target.page);
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		
		if (!(obj instanceof PageRankResult))
			return false;
		
		PageRankResult pr = (PageRankResult) obj;
		
		if (rank - pr.rank < 0.0000001 && page.equals(pr.page))
			return true;
		
		return false;
	}

	public void readFields(DataInput in) throws IOException {
		page = in.readUTF();
		rank = in.readDouble();
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(page);
		out.writeDouble(rank);
	}

	public String toString() {
		return page + "\t" + rank;
	}
}
