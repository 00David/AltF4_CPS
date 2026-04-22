package fr.sorbonne_u.filters;

import java.util.HashMap;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI.PropertyI;

/**
 * Class containing all the filters to apply to the values of a {@link MessageI}'s properties, via the {@code match} method.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code propertyFilters != null}
 * invariant	{@code propertyFiltersLength >= 0}
 * invariant	{@code propertiesFilters != null}
 * invariant	{@code timeFilter != null}
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class MessageFilter implements MessageFilterI{

	private static final long serialVersionUID = -6067517238603360377L;
	
	/** Filters acting on individual message properties */
	private final PropertyFilterI[] propertyFilters;
	
	/** Filters acting on grouped message properties */
	private final PropertiesFilterI[] propertiesFilters;
	
	/** Filter acting on a message's timestamp */
	private final TimeFilterI timeFilter;
	
	/**
	 * Constructs a message filter.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code propertyFilters != null}
	 * pre	{@code propertiesFilters != null}
	 * pre	{@code timeFilter != null}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param propertyFilters 				The set of individual property filters to apply to the values of the message properties.
	 * @param propertiesFilters 			The set of grouped property filters to apply to the values of the message properties.
	 * @param timeFilter 					A time filter to apply to the message timestamp. Can be {@code null}.
	 * @throws IllegalArgumentException		if a precondition is not satisfied.
	 */
	public MessageFilter(PropertyFilterI[] propertyFilters, PropertiesFilterI[] propertiesFilters, TimeFilterI timeFilter) {
		// Precondition checks
		if (propertyFilters == null || propertiesFilters == null) 
			throw new IllegalArgumentException("An array cannot be null");
		if (timeFilter == null) 
			throw new IllegalArgumentException("Time filter cannot be null");
		
		this.propertyFilters = propertyFilters;
		this.propertiesFilters = propertiesFilters;
		this.timeFilter = timeFilter;
	}
	
	// Getters postconditions are satisfied by class invariants

	@Override
	public PropertyFilterI[] getPropertyFilters() {
		return propertyFilters;
	}

	@Override
	public PropertiesFilterI[] getPropertiesFilters() {
		return propertiesFilters;
	}

	@Override
	public TimeFilterI getTimeFilter() {
		return timeFilter;
	}

	@Override
	public boolean match(MessageI message) {
	    boolean matched = true;

	    // 1. Time filter
	    if (!timeFilter.match(message.getTimeStamp())) {
	    	matched = false;
	    }

	    PropertyI[] properties = message.getProperties();
	    HashMap<String, PropertyI> propertiesMap = null;
	    // constructs the map only if it has previously matched, otherwise it will directly go to the return
	    if (matched) {
	    	propertiesMap = new HashMap<>();
	    	for (PropertyI p : properties) {
		    	propertiesMap.put(p.getName(), p);
		    }
	    }

	    // 2. Filters on individual properties
	    for (int i = 0; matched && i<propertyFilters.length; i++) {
	    	PropertyFilterI pf = propertyFilters[i];
	    	String filterPropertyName = pf.getName();
	    	
	    	// Check if property exists AND matches
	        if (!propertiesMap.containsKey(filterPropertyName) || 
	            !pf.match(propertiesMap.get(filterPropertyName))) {
	        	matched = false;
	        }
	    }

	    // 3. Filters on joint properties
	    for (int i = 0; matched && i<propertiesFilters.length; i++) {
	    	PropertiesFilterI pf = propertiesFilters[i];
	        if (!pf.match(properties)) {
	        	matched = false;
	        }
	    }

	    return matched;
	}
	
}