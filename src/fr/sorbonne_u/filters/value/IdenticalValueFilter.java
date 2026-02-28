package fr.sorbonne_u.filters.value;

import java.io.Serializable;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.ValueFilterI;

/**
 * Filter keeping only the values identical to the {@code filterValue} given in the constructor.
 * 
 * <p>
 * Uses the {@code match} method to verify this, which itself uses the {@code equals} method to verify the identical value.
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code filterValue != null}
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class IdenticalValueFilter implements ValueFilterI {

	private static final long serialVersionUID = 8651615691590385567L;
	private Serializable filterValue; 
	
	/**
	 * Constructs the filter keeping only the values identical to {@code filterValue}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code filterValue != null}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param filterValue The only accepted value of the filter.
	 * @throws IllegalArgumentException if {@code filterValue} == {@code null}.
	 */
	public IdenticalValueFilter(Serializable filterValue) {
		// Precondition check
		if (filterValue == null) throw new IllegalArgumentException("A filter value cannot be null");
		this.filterValue = filterValue;
	}
	
	@Override
	public boolean match(Serializable value){
		return filterValue.equals(value);
	}

}
