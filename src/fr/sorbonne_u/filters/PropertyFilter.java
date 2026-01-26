package fr.sorbonne_u.filters;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertyFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.ValueFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI.PropertyI;

public class PropertyFilter implements PropertyFilterI {

	private static final long serialVersionUID = -7695552208630589978L;
	
	private String propertyName;
	private ValueFilterI valueFilter;
	
	public PropertyFilter(String propertyName, ValueFilterI valueFilter) {
		this.propertyName = propertyName;
		this.valueFilter = valueFilter;
	}

	@Override
	public String getName() {
		return propertyName;
	}

	@Override
	public ValueFilterI getValueFilter() {
		return valueFilter;
	}

	@Override
	public boolean match(PropertyI property) {
		return property.getName().equals(propertyName) && valueFilter.match(property.getValue());
	}

}
