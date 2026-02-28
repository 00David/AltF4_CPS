package fr.sorbonne_u.filters.value;

import java.io.Serializable;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.MultiValuesFilterI;

/**
 * Abstract class representing a filter that takes multiple values in its {@code match} method.
 * 
 * <p>
 * For a concrete filter, a class extending this one must be created. Example with: {@link fr.sorbonne_u.filters.tests.utils.IMC}.
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code propertyNames != null && propertyNames.length >= 2}
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public abstract class AbstractMultiValuesFilter implements MultiValuesFilterI {

	private static final long serialVersionUID = 1955588007790725754L;
	protected String[] propertyNames;
	
	/**
	 * Creates a filter applying to multiple property values, with the given {@code propertyNames}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code propertyNames != null && propertyNames.length >= 2}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param propertyNames					The names of the properties whose values correspond in {@code match}.
	 * @throws IllegalArgumentException		if fewer than 2 properties are targeted.
	 */
	public AbstractMultiValuesFilter(String[] propertyNames) {
		// Precondition checks
        if (propertyNames == null || propertyNames.length < 2) {
            throw new IllegalArgumentException( "A multi-values filter must target at least two properties");
        }
		this.propertyNames = propertyNames;
	}
	
	/**
	 * Returns the array of property names which values are the target of the filter.
	 * 
	 * <p>The property names array cannot be null, and contains necessarily at least 2 property names.</p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return != null && return.length >= 2}
	 * </pre>
	 *
	 * @return	the array of property names which values are the target of the filter.
	 */
	@Override
	public String[] getNames() {
		return propertyNames;
	}

	@Override
	public abstract boolean match(Serializable... values);
	
}