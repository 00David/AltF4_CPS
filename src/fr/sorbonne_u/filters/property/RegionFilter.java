package fr.sorbonne_u.filters.property;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.ValueFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI.PropertyI;


public class RegionFilter extends PropertyFilter {

	private static final long serialVersionUID = 1L;

	public RegionFilter(String propertyName, ValueFilterI valueFilter) {
		super(propertyName, valueFilter);
		
	}

	
	/**
	 * Match si la position est dans la région
	 */
	@Override
	public boolean match(PropertyI property) {
		return valueFilter.match(property.getValue());
		
	}
		
	
	

}
