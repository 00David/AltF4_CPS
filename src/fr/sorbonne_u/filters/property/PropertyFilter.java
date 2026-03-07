package fr.sorbonne_u.filters.property;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertyFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.ValueFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI.PropertyI;

/**
 * Filters the value of a single property.
 * 
 * <p>
 * The property is passed to the {@code match} method as a {@link PropertyI}, 
 * while the property name and the associated {@link ValueFilterI} are provided as constructor arguments.
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code propertyName != null && !propertyName.isEmpty()}
 * invariant	{@code valueFilter != null}
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class PropertyFilter implements PropertyFilterI {

    private static final long serialVersionUID = -7695552208630589978L;
    
    /** The name of the property on which the filter will act */
    private String propertyName;
    
    /** The filter acting on the property */
    private ValueFilterI valueFilter;
    
    /**
     * Creates a filter on a specific property.
     * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code propertyName != null && !propertyName.isEmpty()}
	 * pre	{@code valueFilter != null}
	 * post	class invariants satisfied
	 * </pre>
     * 
     * @param propertyName 					The name of the property to filter.
     * @param valueFilter					The filter applied to the value of the property.
     * @throws IllegalArgumentException		if a precondition is not satisfied
     */
    public PropertyFilter(String propertyName, ValueFilterI valueFilter) {
        if (propertyName == null || propertyName.isEmpty()) 
            throw new IllegalArgumentException("A property name cannot be null or empty");
        if (valueFilter == null) 
            throw new IllegalArgumentException("A value filter cannot be null");
        this.propertyName = propertyName;
        this.valueFilter = valueFilter;
    }
    
    // Getters postconditions satisfied by class invariants

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
        return propertyName.equals(property.getName()) 
               && valueFilter.match(property.getValue());
    }
}