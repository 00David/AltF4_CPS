package fr.sorbonne_u.filters.time;

import java.time.Instant;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.TimeFilterI;

/**
 * Time filter keeping the {@code timestamp} (from the {@code match} method) &lt;= {@code timestampFilter} (from the constructor).
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code timestampFilter != null}
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class LessOrEqualTimeFilter implements TimeFilterI{

	private static final long serialVersionUID = 577294073745272893L;
	
	/** The filter accepts only timestamps smaller or equal to this time */
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
	 * @param timestampFilter				Upper time bound of the filter.
	 * @throws IllegalArgumentException		if {@code timestampFilter} == {@code null}.
	 */
	public LessOrEqualTimeFilter(Instant timestampFilter) {
        if (timestampFilter == null) throw new IllegalArgumentException("Timestamp cannot be null");
		this.timestampFilter = timestampFilter;
	}
	
	@Override
	public boolean match(Instant timestamp) {
		if (timestamp == null) return false;
	    return !timestamp.isAfter(timestampFilter); // <=
	}

}