import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class RemoveStopWords {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		String stopWordsFile=args[1], wordsFile=args[0];
		Set<String> stopWords=new HashSet<String>();
		BufferedReader reader=new BufferedReader(new FileReader(stopWordsFile));
		String line=null;
		while( (line=reader.readLine()) != null){
			line=line.toLowerCase();
			String tokens[]=line.split(" ");
			for(String t:tokens){
				stopWords.add(t);
			}
		}
		System.out.println(stopWords.size());
		reader.close();
		reader=new BufferedReader(new FileReader(wordsFile));
		BufferedWriter writer=new BufferedWriter(new FileWriter("newWordsCount.txt"));
		String orgLine=null;
		while( (line=reader.readLine()) != null){
			orgLine=line;
			line=line.toLowerCase();
			String [] tokens=line.split(" ");
			if( ! stopWords.contains(tokens[0])){
				writer.write(orgLine+"\n");
			}
		}
		writer.close();
		reader.close();
	}

}
