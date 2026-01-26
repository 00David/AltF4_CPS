package fr.sorbonne_u.filters;

import java.io.Serializable;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.ValueFilterI;

public class ValueFilter implements ValueFilterI{

	private static final long serialVersionUID = 8790520295565397756L;
	
	private Serializable filterValue; 
	
	public ValueFilter(Serializable filterValue) {
		this.filterValue = filterValue;
	}

	@Override
	public boolean match(Serializable value) {
		return filterValue.equals(value);
	}

}
