package fr.sorbonne_u.filters.value.comparable;

import java.io.Serializable;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.ValueFilterI;

/**
 * Filter keeping only the values equal to the {@code filterValue} given in the constructor.
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
public class EqualToValueFilter<T extends Comparable<T>> implements ValueFilterI {

	private static final long serialVersionUID = 8637244812712629733L;
	
	/** The filter accepts only values equal to this value */
	private T filterValue; 
	
	/**
	 * Constructs the filter keeping values equal to {@code filterValue}, of a type implementing {@link Comparable}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code filterValue != null}
	 * post	class invariant satisfied
	 * </pre>
	 * 
	 * @param filterValue 					The filter value.
	 * @throws IllegalArgumentException 	if {@code filterValue} == {@code null}.
	 */
	public EqualToValueFilter(T filterValue) {
		// Precondition check
		if (filterValue == null) throw new IllegalArgumentException("A filter value cannot be null");
		this.filterValue = filterValue;
	}
	
	@Override
	public boolean match(Serializable value) throws ClassCastException {
		if (value == null) return false;
		T v = (T)value; // ClassCastException possible here, same type as filterValue's type needed
		return v.compareTo(filterValue) == 0;
	}

}
