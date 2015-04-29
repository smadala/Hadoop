
public class GrepJob implements IMapper, IReducer{

	@Override
	public String reduce(String s) {
		// TODO Auto-generated method stub
		
		return s;
	}

	@Override
	public String map(String s) {
		String res=null;
		if(s != null){
			if(s.contains("MapReduce"))
				res=s;
		}
		return res;
	}
	
	

}
