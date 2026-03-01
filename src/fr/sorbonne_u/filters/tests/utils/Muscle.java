package fr.sorbonne_u.filters.tests.utils;

import java.io.Serializable;

import fr.sorbonne_u.filters.value.AbstractMultiValuesFilter;

/**
 * Concrete example class for a multi-values filter.<br>
 * Considers a person as muscular (match returns true) for the total weight and muscular weight of a person,
 * if the muscular weight represents at least 40% of the person's total weight.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class Muscle extends AbstractMultiValuesFilter{

	private static final long serialVersionUID = -5177956127310642977L;

	/**
	 * Constructs our test muscle mass class, directly assigning the 2 relevant property names.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true} // no precondition
	 * post	{@code true} // no postcondition
	 * </pre>
	 */
	public Muscle() {
		super(new String[]{"weight", "muscle_weight"});
	}

	/**
	 * @throws IllegalArgumentException if {@code values} == {@code null} or {@code values} has less than 2 values.
	 */
	@Override
	public boolean match(Serializable... values) {
		// Precondition checks
		if (values == null) throw new IllegalArgumentException("values cannot be null");
		if (values.length < 2) throw new IllegalArgumentException("A multi-values filter cannot have less than 2 values");
		
		if (!(values[0] instanceof Double)) return false;
		double p = (double)values[0];
		if (!(values[1] instanceof Double)) return false;
		double m = (double)values[1];
		
		return (m/p)*100.0 >= 40.0;
	}

}