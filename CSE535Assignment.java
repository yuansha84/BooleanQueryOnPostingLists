import java.io.*;
import java.util.*;

public class CSE535Assignment {

	//the first parameter is the index file, the second parameter is the log file name, the third is an 
	//integer that will be used in getTopK, the last parameter is a file containing query terms
	public static void main(String[] args){
		if(args.length!=4){
			System.out.println("Invalid number of arguments, 4 arguments required.");
			System.exit(-1);
		}
		QueryEngine engine=new QueryEngine(args[0],args[1],args[2],args[3]);
		engine.start();
	}
	
	
}
