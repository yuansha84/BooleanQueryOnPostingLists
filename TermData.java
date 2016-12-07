import java.util.*;

public class TermData {

	int size;
	LinkedList<Posting> postings;
	
	public TermData(int s,LinkedList<Posting> p){
		size=s;
		postings=p;
	}
	
	//getters
	int getSize(){
		return size;
	}
	
	LinkedList<Posting> getPostings(){
		return postings;
	}
	
}
