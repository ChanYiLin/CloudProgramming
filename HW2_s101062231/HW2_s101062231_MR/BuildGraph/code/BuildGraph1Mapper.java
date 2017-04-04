//BuildGraph1Mapper.java
package BuildGraph;
import java.util.ArrayList;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.nio.charset.CharacterCodingException;
import org.apache.hadoop.conf.Configuration;


import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BuildGraph1Mapper extends Mapper<LongWritable, Text, Text, Text> {


    //private static final Pattern wikiLinksPattern = Pattern.compile("\\[.+?\\]");
    //private static final Pattern wikiLinksRegex = Pattern.compile("\\[\\[(.*?)([\\|#]|\\]\\])");
    //private static final Pattern wikiTitleRegex = Pattern.compile("<title>(.+?)</title>");
    final static private Pattern titlePattern = Pattern.compile("<title>(.+?)</title>");
    final static private Pattern linkPattern = Pattern.compile("\\[\\[(.*?)([\\|#]|\\]\\])");
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        Matcher titleMatcher = titlePattern.matcher(line);
        Matcher linkMatcher = linkPattern.matcher(line);
            
        if ( titleMatcher.find() ){
            String title = replaceSpecialString(titleMatcher.group(1));
            title = title.replaceAll("<title>|</title>", "");
            title = capitalizeFirstLetter(title);
            //localTotalNodeCount += 1;
            // pass deadend information
            //outputKey.set(title);
            //outputValue.set("");
            context.write(new Text(title), new Text(title));

            while( linkMatcher.find() ){
                String link = replaceSpecialString(linkMatcher.group(1));
                link = link.replaceAll("\\[\\[|\\]\\]|\\||#", "");
                if(link == null || link.isEmpty()) 
                    continue;
                link = capitalizeFirstLetter(link);
                //outputValue.set(link);
                if(link.equals(title)){
                    context.write(new Text(link+"#"),new Text(title));//link -> title
                }else{
                    context.write(new Text(link),new Text(title));//link -> title
                }
                
                //context.write(new Text(link), new Text(title));
            }


        }
        else{
            throw new IOException("MYERROR: input doesn't have a title");
        }

    }
    private String replaceSpecialString(String input){
        return input.replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&").replaceAll("&quot;", "\"").replaceAll("&apos;", "'");
    }

    private String capitalizeFirstLetter(String input){
        char firstChar = input.charAt(0);
        if ( (firstChar >= 'a' && firstChar <='z') || (firstChar>= 'A' && firstChar <= 'Z') ){
            if ( input.length() == 1 ){
                return input.toUpperCase();
            }
            else{
                return input.substring(0, 1).toUpperCase() + input.substring(1);
            }
        }
        else{
            return input;
        }
    }
   

}

