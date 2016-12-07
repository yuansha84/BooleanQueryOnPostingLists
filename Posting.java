
public class Posting implements Comparable<Posting>{

	int docID;
	int tf;
	
	public Posting(int id,int f){
		docID=id;
		tf=f;
	}
	
	//getters
	int getDocID(){
		return docID;
	}
	
	int getTf(){
		return tf;
	}
	
	//compareTo
	public int compareTo(Posting p2){
		if(tf<p2.getTf())
			return -1;
		else if(tf>p2.getTf())
			return 1;
		else return 0;
	}
	
}
