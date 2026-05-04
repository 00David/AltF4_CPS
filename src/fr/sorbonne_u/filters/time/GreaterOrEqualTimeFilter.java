package fr.sorbonne_u.filters.time;

import java.time.Instant;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.TimeFilterI;

/**
 * Time filter keeping the {@code timestamp} (from the {@code match} method) >= {@code timestampFilter} (from the constructor).
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code timestampFilter != null}
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class GreaterOrEqualTimeFilter implements TimeFilterI{

	private static final long serialVersionUID = -7998024328704643904L;
	
	/** The filter accepts only timestamps greater or equal to this time */
	private Instant timestampFilter;
	
	/**
	 * Constructs the filter, with the {@code timestampFilter} used in {@code match}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code timestampFilter != null}
	 * post	class invariant satisfied
	 * </pre>
	 * 
	 * @param timestampFilter				Lower time bound of the filter.
	 * @throws IllegalArgumentException		if {@code timestampFilter} == {@code null}.
	 */
	public GreaterOrEqualTimeFilter(Instant timestampFilter) {
		// Precondition check
        if (timestampFilter == null) 
        	throw new IllegalArgumentException("Timestamp cannot be null");
        
		this.timestampFilter = timestampFilter;
	}
	
	@Override
	public boolean match(Instant timestamp) {
		if (timestamp == null) return false;
	    return !timestamp.isBefore(timestampFilter); // >=
	}

}