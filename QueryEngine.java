import java.io.*;
import java.util.*;


public class QueryEngine {

	public QueryEngine(String index,String log,String k,String que){
		indexFile=index;
		logFile=log;
		topK=Integer.parseInt(k);
		queries=que;
	}
	
	public void start(){
		createIndex(indexFile);
		try{
			BufferedWriter writer=new BufferedWriter(new FileWriter(logFile));
			getTopK(writer);
			BufferedReader reader=new BufferedReader(new FileReader(queries));
			String line;
			while((line=reader.readLine())!=null){
				String[] terms=line.split(" ");
				getPostings(terms,writer);
				termAtATimeQueryAnd(terms,writer);
				termAtATimeQueryOr(terms,writer);
				docAtATimeQueryAnd(terms,writer);
				docAtATimeQueryOr(terms,writer);
			}
			reader.close();
			writer.close();		
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	void getTopK(BufferedWriter writer){
		String[] terms=new String[topK];
		Iterator<String> iter=index_main.keySet().iterator();
		int size=0;
		/*System.out.println(index_main.keySet().size());
		while(iter.hasNext()){
			System.out.println(iter.next());
		}*/
		while(iter.hasNext()){
			String key=iter.next();
			TermData td=index_main.get(key);
			int df=td.getSize();
			if(size<topK){//insert into the sorted array terms
				int i;
				for(i=size-1;i>=0;i--)
					if(df>index_main.get(terms[i]).getSize())
						terms[i+1]=terms[i];
					else break;
				terms[i+1]=key;
				size++;
			}
			else{
				int i;
				if(df>index_main.get(terms[topK-1]).getSize()){
				for(i=topK-2;i>=0;i--)
					if(df>index_main.get(terms[i]).getSize())
						terms[i+1]=terms[i];
					else break;
				terms[i+1]=key;
			}
			}
		}
		try{
		writer.write("FUNCTION: getTopK "+topK);
		writer.newLine();
		String result="Result: ";
		for(int i=0;i<topK;i++){
			if(i<topK-1)
			result+=terms[i]+", ";
			else result+=terms[i];
		}
		writer.write(result);
		writer.newLine();
		/*if(index_main.get("net")!=null)
			System.out.println(index_main.get("net").getSize());
		System.out.println(result);*/
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	void getPostings(String[] terms,BufferedWriter writer){
		String term="";
		LinkedList<Posting> post_id,post_tf;
		try{
		for(int i=0;i<terms.length;i++){
			term=terms[i];
			if(!index_main.containsKey(terms[i])){
				writer.write("FUNCTION: getPostings "+term);
				writer.newLine();
				writer.write("term not found\n");
			}else{
			post_id=index_main.get(term).getPostings();
			post_tf=index_tf.get(term).getPostings();
			String result_id="Ordered by doc IDs: ";
			String result_tf="Ordered by TF: ";
			for(int j=0;j<post_id.size();j++){
				if(j<post_id.size()-1){
				result_id+=post_id.get(j).getDocID()+", ";
				result_tf+=post_tf.get(j).getDocID()+", ";
				}else{
					result_id+=post_id.get(j).getDocID();
					result_tf+=post_tf.get(j).getDocID();
				}
			}
			writer.write("FUNCTION: getPostings "+term);
			writer.newLine();
			writer.write(result_id);
			writer.newLine();
			writer.write(result_tf);
			writer.newLine();
			}
		}
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	//termAtATimeQueryAnd functions
	void termAtATimeQueryAnd(String[] terms,BufferedWriter writer){
		int comp_num=0;
		long start,end;
		ArrayList<LinkedList<Posting>> ps=new ArrayList<LinkedList<Posting>>();
		LinkedList<Posting> lst=new LinkedList<Posting>();
		String result;
		try{
		for(int j=0;j<terms.length;j++){
			if(!index_main.containsKey(terms[j])){//if the term is not present in the index
				result="FUNCTION: termAtATimeQueryAnd ";
				for(int i=0;i<terms.length;i++)
					if(i<terms.length-1)
						result+=terms[i]+", ";
					else result+=terms[i]+'\n';
				result+="terms not found\n";
				writer.write(result);
				return;
			}
			//else add its posting list to ps
			ps.add(index_tf.get(terms[j]).getPostings());
		}
		start=System.currentTimeMillis();
		comp_num=postIntersection(ps,lst);
		end=System.currentTimeMillis();
		long elapsed=(end-start)/1000;//the elapsed time measured in seconds
		Collections.sort(lst,compar1);//sort into ascending doc ID order
		//compute the result string
		result="FUNCTION: termAtATimeQueryAnd ";
		for(int i=0;i<terms.length;i++)
			if(i<terms.length-1)
				result+=terms[i]+", ";
			else result+=terms[i]+'\n';
		result+=lst.size()+" documents are found\n";
		result+=comp_num+" comparisons are made\n";
		result+=elapsed+" seconds are used\n";
		//with optimization
		int comp_opt=termAtATimeQueryAndOptimize(terms);
		result+=comp_opt+" comparisons are made with optimization\n";
		result+="Result: ";
		for(int i=0;i<lst.size();i++)
			if(i<lst.size()-1)
				result+=lst.get(i).getDocID()+", ";
			else result+=lst.get(i).getDocID();
		result+='\n';
		writer.write(result);
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	/*function: int postIntersection(ArrayList<LinkedList<Posting>> lists ,LinkedList<Posting> lst);
	 This function computes the intersection of lists, put the result in lst and return the number of comparisons made.
	 lst should be empty initially.
	 */
	int postIntersection(ArrayList<LinkedList<Posting>> lists,LinkedList<Posting> lst){
		lst.addAll(lists.get(0));
		int comp_num=0;
		LinkedList<Posting> tmp=new LinkedList<Posting>();
		for(int i=1;i<lists.size();i++){
			LinkedList<Posting> sec=lists.get(i);//the current other list to check for intersection
			for(int j=0;j<lst.size();j++){//check for each docID in lst to see if it is present in sec
				int docID=lst.get(j).getDocID();
				int k;
				for(k=0;k<sec.size()&&docID!=sec.get(k).getDocID();k++,comp_num++);//comp_num++ whenever a comparison is made
				if(k<sec.size())//found in sec,add to tmp
					tmp.add(lst.get(j));
			}
			lst.clear();
			lst.addAll(tmp);
			tmp.clear();
		}
		return comp_num;
	}
	
	int termAtATimeQueryAndOptimize(String[] terms){
		//LinkedList<Posting>[] ps=new LinkedList<Posting>[terms.length];
		ArrayList<LinkedList<Posting>> ps=new ArrayList<LinkedList<Posting>>();
		ArrayList<TermData> tds=new ArrayList<TermData>();
		for(int i=0;i<terms.length;i++)
			tds.add(index_tf.get(terms[i]));
		Collections.sort(tds,compar2);//sort the posting lists into ascending size order
		for(int i=0;i<tds.size();i++)
			ps.add(tds.get(i).getPostings());
		LinkedList<Posting> lst=new LinkedList<Posting>();
		return postIntersection(ps,lst);
	}
	
	
	//termAtATimeQueryOr functions
	void termAtATimeQueryOr(String[] terms,BufferedWriter writer){
		int comp_num=0;
		long start,end;
		ArrayList<LinkedList<Posting>> ps=new ArrayList<LinkedList<Posting>>();
		LinkedList<Posting> lst=new LinkedList<Posting>();
		String result;
		try{
		for(int i=0;i<terms.length;i++){
			if(!index_main.containsKey(terms[i])){
				result="FUNCTION: termAtATimeQueryOr ";
				for(int j=0;j<terms.length;j++)
					if(i<terms.length-1)
						result+=terms[j]+", ";
					else result+=terms[j]+'\n';
				result+="terms not found\n";
				writer.write(result);
				return;
			}
			ps.add(index_tf.get(terms[i]).getPostings());
		}
		start=System.currentTimeMillis();
		comp_num=postUnion(ps,lst);
		end=System.currentTimeMillis();
		long elapsed=(end-start)/1000;//the elapsed time measured in seconds
		Collections.sort(lst,compar1);//sort into ascending doc ID order
		//computes the result string
		result="FUNCTION: termAtATimeQueryOr ";
		for(int i=0;i<terms.length;i++)
			if(i<terms.length-1)
				result+=terms[i]+", ";
			else result+=terms[i]+'\n';
		result+=lst.size()+" documents are found\n";
		result+=comp_num+" comparisons are made\n";
		result+=elapsed+" seconds are used\n";
		//with optimization
		int comp_opt=termAtATimeQueryOrOptimize(terms);
		result+=comp_opt+" comparisons are made with optimization\n";
		result+="Result: ";
		for(int i=0;i<lst.size();i++)
			if(i<lst.size()-1)
				result+=lst.get(i).getDocID()+", ";
			else result+=lst.get(i).getDocID();
		result+='\n';
		writer.write(result);
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	/*funct: int postUnion(ArrayList<LinkedList<Posting>> lists,LinkedList<Posting> lst);
	 * this function computes the union of lists ,put the result in lst and return the number of comparisons
	 * lst should be empty initially
	 */
	int postUnion(ArrayList<LinkedList<Posting>> lists,LinkedList<Posting> lst){
		lst.addAll(lists.get(0));
		int comp_num=0;
		LinkedList<Posting> tmp=new LinkedList<Posting>();
		for(int i=1;i<lists.size();i++){//check for unpresent docID in  each other posting list
			LinkedList<Posting> sec=lists.get(i);
			for(int j=0;j<sec.size();j++){
				int docID=sec.get(j).getDocID();
				int k;
				for(k=0;k<lst.size()&&docID!=lst.get(k).getDocID();k++,comp_num++);//comp_num++ whenever a comparison is made
				if(k>=lst.size())//not found in lst,add to tmp
					tmp.add(sec.get(j));
			}
			lst.addAll(tmp);
			tmp.clear();
		}
		return comp_num;
	}
	
	int termAtATimeQueryOrOptimize(String[] terms){
		//LinkedList<Posting>[] ps=new LinkedList<Posting>[terms.length];
		ArrayList<LinkedList<Posting>> ps=new ArrayList<LinkedList<Posting>>();
		ArrayList<TermData> tds=new ArrayList<TermData>();
		for(int i=0;i<terms.length;i++)
			tds.add(index_tf.get(terms[i]));
		Collections.sort(tds,compar2);
		for(int i=0;i<tds.size();i++)
			ps.add(tds.get(i).getPostings());
		LinkedList<Posting> lst=new LinkedList<Posting>();
		return postUnion(ps,lst);
	}
	
	//docAtATimeQueryAnd functions
	void docAtATimeQueryAnd(String[] terms,BufferedWriter writer){
		ArrayList<LinkedList<Posting>> pts=new ArrayList<LinkedList<Posting>>();//store the posting lists
		int[] curp=new int[terms.length];//the current index(or pointer) of the posting lists
		String result;
		long start,end;
		start=System.currentTimeMillis();
		try{
		for(int i=0;i<terms.length;i++){
			if(!index_main.containsKey(terms[i])){//if the term is not present in index_main,writes result and return
				result="FUNCTION: docAtATimeQueryAnd ";
				for(int j=0;j<terms.length;j++)
					if(i<terms.length-1)
						result+=terms[j]+", ";
					else result+=terms[j]+'\n';
				result+="terms not found\n";
				writer.write(result);
				return;
			}
			pts.add(index_main.get(terms[i]).getPostings());
			curp[i]=0;
		}
		ArrayList<Integer> minDocs=new ArrayList<Integer>();//store the indexes for posting lists which have the current minimum doc ID
		LinkedList<Posting> lst=new LinkedList<Posting>();
		int comp_num=0;
		OuterLoop:
		while(true){
			for(int i=0;i<pts.size();i++)
				if(curp[i]>=pts.get(i).size())//running out of a posting list, break out since this is an intersection
					break OuterLoop;
			minDocs.clear();
			minDocs.add(0);
			for(int i=1;i<pts.size();i++){
				int minID=pts.get(minDocs.get(0)).get(curp[minDocs.get(0)]).getDocID();
				int curID=pts.get(i).get(curp[i]).getDocID();
				if(minID>curID){//find a smaller doc ID
					minDocs.clear();
					minDocs.add(i);
				}else if(minID==curID)
					minDocs.add(i);
				comp_num++;
			}
			if(minDocs.size()==terms.length)//this doc ID is contained in all the posting lists,add it
				lst.add(pts.get(minDocs.get(0)).get(curp[minDocs.get(0)]));
			for(int i=0;i<minDocs.size();i++){
				curp[minDocs.get(i)]++;
			}
		}
		end=System.currentTimeMillis();
		long elapsed=(end-start)/1000;
		//computes the result string
		result="FUNCTION: docAtATimeQueryAnd ";
		for(int i=0;i<terms.length;i++)
			if(i<terms.length-1)
				result+=terms[i]+", ";
			else result+=terms[i]+'\n';
		result+=lst.size()+" documents are found\n";
		result+=comp_num+" comparisons are made\n";
		result+=elapsed+" seconds are used\n";
		result+="Result: ";
		for(int i=0;i<lst.size();i++)
			if(i<lst.size()-1)
				result+=lst.get(i).getDocID()+", ";
			else result+=lst.get(i).getDocID();
		result+='\n';
		writer.write(result);
		//writer.flush();
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	//docAtATimeQueryOr function
	void docAtATimeQueryOr(String[] terms,BufferedWriter writer){
		ArrayList<LinkedList<Posting>> pts=new ArrayList<LinkedList<Posting>>();//store the posting lists
		ArrayList<LinkedList<Posting>> tmp=new ArrayList<LinkedList<Posting>>();//store the posting lists that remain after processing a doc ID
		int[] curp=new int[terms.length];//the current index(or pointer) of the posting lists
		int[] tmpp=new int[terms.length];//the current index(or pointer) of the posting lists that remain after processing a doc ID
		String result;
		long start,end;
		start=System.currentTimeMillis();
		try{
		for(int i=0;i<terms.length;i++){
			if(!index_main.containsKey(terms[i])){
				result="FUNCTION: docAtATimeQueryOr ";
				for(int j=0;j<terms.length;j++)
					if(i<terms.length-1)
						result+=terms[j]+", ";
					else result+=terms[j]+'\n';
				result+="terms not found\n";
				writer.write(result);
				return;
			}
			pts.add(index_main.get(terms[i]).getPostings());
			curp[i]=0;
		}
		ArrayList<Integer> minDocs=new ArrayList<Integer>();//the indexes of the posting lists that contains the current min docID
		LinkedList<Posting> lst=new LinkedList<Posting>();
		int comp_num=0;
		while(true){
			if(pts.size()==0)
				break;
			minDocs.clear();
			minDocs.add(0);
			for(int i=1;i<pts.size();i++){
				int minID=pts.get(minDocs.get(0)).get(curp[minDocs.get(0)]).getDocID();
				int curID=pts.get(i).get(curp[i]).getDocID();
				if(minID>curID){
					minDocs.clear();
					minDocs.add(i);
				}else if(minID==curID)
					minDocs.add(i);
				comp_num++;
			}
			//System.out.println(minDocs.size());
			//System.out.println(curp[minDocs.get(0)]);
			lst.add(pts.get(minDocs.get(0)).get(curp[minDocs.get(0)]));
			for(int i=0;i<minDocs.size();i++){
				curp[minDocs.get(i)]++;
			}
			for(int i=0,j=0;i<pts.size();i++)
				if(curp[i]<pts.get(i).size()){
					tmp.add(pts.get(i));
					tmpp[j++]=curp[i];
				}
			pts.clear();
			for(int i=0;i<tmp.size();i++){
				pts.add(tmp.get(i));
				curp[i]=tmpp[i];
			}
			tmp.clear();
		}
		end=System.currentTimeMillis();
		long elapsed=(end-start)/1000;
		//computes the result string
		result="FUNCTION: docAtATimeQueryOr ";
		for(int i=0;i<terms.length;i++)
			if(i<terms.length-1)
				result+=terms[i]+", ";
			else result+=terms[i]+'\n';
		result+=lst.size()+" documents are found\n";
		result+=comp_num+" comparisons are made\n";
		result+=elapsed+" seconds are used\n";
		result+="Result: ";
		for(int i=0;i<lst.size();i++)
			if(i<lst.size()-1)
				result+=lst.get(i).getDocID()+", ";
			else result+=lst.get(i).getDocID();
		result+='\n';
		writer.write(result);
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	private void createIndex(String file){
		String line;
		index_main=new HashMap<String,TermData>();
		index_tf=new HashMap<String,TermData>();
		try{
		BufferedReader bufferReader=new BufferedReader(new FileReader(file));
		while((line=bufferReader.readLine())!=null){//read one line and add the term data
			String[] comp=line.split("\\\\c|\\\\m");
			/*for(int i=0;i<comp.length;i++)
			System.out.println(comp[i]);*/
			String term=comp[0];
			int size=Integer.parseInt(comp[1]);
			String[] pos=comp[2].substring(1,comp[2].length()-1).split(", ");
			/*for(int i=0;i<pos.length;i++)
				System.out.println(pos[i]);*/
			LinkedList<Posting> lst=new LinkedList<Posting>();
			for(int i=0;i<pos.length;i++){
				String[] doc=pos[i].split("\\/");
				/*for(int j=0;j<doc.length;j++)
					System.out.println(doc[j]);*/
				lst.addLast(new Posting(Integer.parseInt(doc[0]),Integer.parseInt(doc[1])));
				
			}
			index_main.put(term, new TermData(size,lst));
			LinkedList<Posting> lst_tf=(LinkedList<Posting>)lst.clone();
			Collections.sort(lst_tf,compar);
			index_tf.put(term, new TermData(size,lst_tf));
			
		}
		bufferReader.close();
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	//this comparator is used to order posting list in decreasing tf order
	Comparator<Posting> compar=new Comparator<Posting>(){
		public int compare(Posting p1,Posting p2){
			if(p1.getTf()>p2.getTf())
				return -1;
			else if(p1.getTf()<p2.getTf())
				return 1;
			else return 0;
		}
	};
	
	//this comparator is used to order posting list in increasing docID order
	Comparator<Posting> compar1=new Comparator<Posting>(){
		public int compare(Posting p1,Posting p2){
			return p1.getDocID()-p2.getDocID();
		}
	};
	
	//this comparator is used to sort posting lists in ascending size order
	Comparator<TermData> compar2=new Comparator<TermData>(){
		public int compare(TermData p1,TermData p2){
			return p1.getSize()-p2.getSize();
		}
	};
	
	String indexFile;
	String logFile;
	int topK;
	String queries;
	//indexes
	HashMap<String,TermData> index_main;
	HashMap<String,TermData> index_tf;
}
