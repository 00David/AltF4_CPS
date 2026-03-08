package fr.sorbonne_u.filters.property;

import java.io.Serializable;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.MultiValuesFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertiesFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI.PropertyI;

/**
 * Filters the values of certain given properties.
 * 
 * <p>
 * The properties are passed to the {@code match} method as a list of {@link PropertyI}, 
 * while the associated {@link MultiValuesFilterI} is provided as a constructor argument.
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code multiValuesFilter != null}
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class PropertiesFilter implements PropertiesFilterI {

    private static final long serialVersionUID = 6231862455176712802L;
    
	/** The filter acting on grouped properties */
    private MultiValuesFilterI multiValuesFilter;
    
    /**
     * Constructs a filter concerning multiple properties.
     * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code multiValuesFilter != null}
	 * post	class invariant satisfied
	 * </pre>
     * 
     * @param multiValuesFilter				The filter applied to the values of the concerned properties.
     * @throws IllegalArgumentException		if {@code multiValuesFilter} is null
     */
    public PropertiesFilter(MultiValuesFilterI multiValuesFilter) {
    	// Precondition check
        if (multiValuesFilter == null) throw new IllegalArgumentException("A multi-values filter cannot be null");
        this.multiValuesFilter = multiValuesFilter;
    }

    @Override
    public MultiValuesFilterI getMultiValuesFilter() {
        return multiValuesFilter;
    }
    
    /**
     * @throws IllegalArgumentException if a precondition is not satisfied
     * @see MessageFilterI#match(fr.sorbonne_u.cps.pubsub.interfaces.MessageI)
     */
    @Override
    public boolean match(PropertyI... properties) {
        // Preconditions check
        if (properties == null) 
            throw new IllegalArgumentException("Properties cannot be null");
        if (properties.length < 2) 
            throw new IllegalArgumentException("At least 2 properties are required");

        String[] propertyNames = multiValuesFilter.getNames();
        Serializable[] values = new Serializable[propertyNames.length];
        
        // For each property name required by the MultiValuesFilterI
        for (int i = 0; i < propertyNames.length; i++) {
            boolean found = false;

            // Search for the corresponding property among the arguments
            for (PropertyI p : properties) {
                if (p.getName().equals(propertyNames[i])) {
                    values[i] = p.getValue();
                    found = true;
                    break;
                }
            }

            if (!found) 
                throw new IllegalArgumentException(
                    "Required property for the filter not found : " + propertyNames[i]
                ); // required property missing
        }

        return multiValuesFilter.match(values);
    }
}