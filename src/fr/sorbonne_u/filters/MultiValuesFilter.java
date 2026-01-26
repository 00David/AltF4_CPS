package fr.sorbonne_u.filters;

import java.io.Serializable;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.MultiValuesFilterI;

public class MultiValuesFilter implements MultiValuesFilterI {

	private static final long serialVersionUID = 1955588007790725754L;

	private String[] propertyNames;
	
	public MultiValuesFilter(String[] propertyNames) {
		this.propertyNames = propertyNames;
	}
	
	@Override
	public String[] getNames() {
		return propertyNames;
	}

	@Override
	public boolean match(Serializable... values) {
		// TODO Auto-generated method stub
		return false;
	}

}
