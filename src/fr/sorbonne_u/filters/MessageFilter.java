package fr.sorbonne_u.filters;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI.PropertyI;

/**
 * Class containing all the filters to apply to the values of a {@link MessageI}'s properties, via the {@code match} method.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code propertyFilters != null}
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
	private PropertyFilterI[] propertyFilters;
	
	/** Filters acting on grouped message properties */
	private PropertiesFilterI[] propertiesFilters;
	
	/** Filter acting on a message's timestamp */
	private TimeFilterI timeFilter;
	
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
	 * @param propertyFilters 				The set of {@link PropertyFilterI} to apply to the values of the message properties.
	 * @param propertiesFilters 			The set of {@link PropertiesFilterI} to apply to the values of the message properties.
	 * @param timeFilter 					A {@link TimeFilterI} to apply to the message timestamp. Can be {@code null}.
	 * @throws IllegalArgumentException		if a precondition is not satisfied..
	 */
	public MessageFilter(PropertyFilterI[] propertyFilters, PropertiesFilterI[] propertiesFilters, TimeFilterI timeFilter) {
		// Precondition checks
		if (propertyFilters == null || propertiesFilters == null) throw new IllegalArgumentException("An array cannot be null");
		if (timeFilter == null) throw new IllegalArgumentException("Time filter cannot be null");
		
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

	    // 1. Time filter
	    if (!timeFilter.match(message.getTimeStamp())) {
	        return false;
	    }

	    PropertyI[] properties = message.getProperties();

	    // 2. Filters on individual properties
	    for (PropertyFilterI pf : propertyFilters) {
	        boolean matched = false;

	        for (PropertyI p : properties) {
	            if (pf.match(p)) {
	                matched = true;
	                break;
	            }
	        }

	        // The property expected by the filter did not match
	        if (!matched) return false;
	    }

	    // 3. Filters on joint properties
	    for (PropertiesFilterI pf : propertiesFilters) {
	        if (!pf.match(properties)) return false;
	    }

	    return true;
	}
	
}