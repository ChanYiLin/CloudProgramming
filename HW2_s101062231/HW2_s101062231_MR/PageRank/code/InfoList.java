package PageRank;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import java.util.StringTokenizer;
import java.util.*;


public class InfoList{
	private String title;
	private double rank, rankDiff;
	private Set<String> outLinks;

	public InfoList() {
		this.title = "";
		this.rank = 0.0;
		this.rankDiff = 0.0;
		this.outLinks = new HashSet<String>();
	}

	public InfoList(String link) {
		StringTokenizer tokenizer = new StringTokenizer(link, "\t");

		// The first token must be the title
		if(tokenizer.hasMoreTokens())
			title = tokenizer.nextToken();
		if(tokenizer.hasMoreTokens())
			rank = Double.parseDouble(tokenizer.nextToken());

		if(tokenizer.hasMoreTokens())
			rankDiff = Double.parseDouble(tokenizer.nextToken());

		// The rest of tokens are the linked nodes
		outLinks = new HashSet<String>();
		while (tokenizer.hasMoreTokens())
			outLinks.add(tokenizer.nextToken());
	}
	public void setTitle(String title) {
		this.title = title;
	}

	public String getTitle() {
		return title;
	}

	public void setRank(double rank) {
		this.rank = rank;
	}

	public double getRank() {
		return rank;
	}
	public void setRankDiff(double rankDiff) {
		this.rankDiff = rankDiff;
	}
	public void addOutLink(String link) {
		outLinks.add(link);
	}

	public void addOutLink(Set<String> links) {
		outLinks.addAll(links);
	}

	public Set<String> getOutlinks() {
		return outLinks;
	}

	public boolean hasSelfLink() {
		return outLinks.contains(title);
	}

	public void deleteSelfLink() {
		outLinks.remove(title);
	}

	public String toString() {
		StringBuilder tmp = new StringBuilder();

		// Print the rank of the page
		tmp.append(rank);
		tmp.append('\t');

		// Print the difference of PageRank
		tmp.append(rankDiff);
		tmp.append('\t');

		// Print the out links of the page
		for (String link : outLinks) {
			tmp.append(link);
			tmp.append('\t');
		}

		return tmp.substring(0, tmp.length() - 1).toString();
	}
}