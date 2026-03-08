package fr.sorbonne_u.filters.value.comparable;

import java.io.Serializable;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.ValueFilterI;

/**
 * Filter keeping only the values greater than the {@code filterValue} given in the constructor.
 * 
 * <p>
 * For types implementing {@link Comparable}. The {@code match} code uses the {@link Comparable#compareTo(Object)} method.
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code filterValue != null}
 * </pre>
 * 
 * @param <T> The type of values compared by the filter, either the filter's reference value or the one given as argument 
 * to {@code match}, both being the same type and implementing {@link Comparable}.
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class GreaterThanValueFilter<T extends Comparable<T>> implements ValueFilterI {

	private static final long serialVersionUID = -7377931202662396565L;
	
	/** The filter accepts only values greater than this value */
	private T filterValue; 
	
	/**
	 * Constructs the filter keeping values greater than {@code filterValue}, of a type implementing {@link Comparable}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code filterValue != null}
	 * post	class invariant satisfied
	 * </pre>
	 * 
	 * @param filterValue 					The filter value.
	 * @throws IllegalArgumentException		if {@code filterValue} == {@code null}.
	 */
	public GreaterThanValueFilter(T filterValue) {
		// Precondition check
		if (filterValue == null) throw new IllegalArgumentException("A filter value cannot be null");
		this.filterValue = filterValue;
	}
	
	@Override
	public boolean match(Serializable value) {
		if (value == null) return false;
		T v = (T)value; // ClassCastException possible here, same type as filterValue's type needed
		return v.compareTo(filterValue) > 0;
	}

}
