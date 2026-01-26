package fr.sorbonne_u.filters;

import java.io.Serializable;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertyFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.ValueFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI.PropertyI;

public class OneIntegerValueFilter implements ValueFilterI {

	private static final long serialVersionUID = 8418130044029451842L;
	private int filterValue; 
	
	public OneIntegerValueFilter(int filterValue) {
		this.filterValue = filterValue;
	}
	
	@Override
	public boolean match(Serializable value){
		if (!(value instanceof Integer)) throw new IllegalArgumentException("Not an integer value");
		
		return false;
	}


}
