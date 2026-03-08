package fr.sorbonne_u.filters.time;

import java.time.Instant;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.TimeFilterI;

/**
 * Time filter keeping the {@code timestamp} (from the {@code match} method) verifying: <br>
 * {@code minBound} <= {@code timestamp} <= {@code maxBound}, with {@code minBound} and {@code maxBound} set in the constructor.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code minBound != null}
 * invariant	{@code maxBound != null}
 * invariant	{@code !maxBound.isBefore(minBound)}
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class IntervalTimeFilter implements TimeFilterI{

	private static final long serialVersionUID = -1021372401821067437L;
	
	/** The lower bound of timestamps accepted by this filter */
	private Instant minBound;
	
	/** The upper bound of timestamps accepted by this filter */
	private Instant maxBound;
	
	/**
	 * Constructs the filter, with {@code minBound} and {@code maxBound} used in {@code match}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code minBound != null}
	 * pre	{@code maxBound != null}
	 * pre	{@code !maxBound.isBefore(minBound)}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param minBound 						Lower time bound of the filter.
	 * @param maxBound 						Upper time bound of the filter.
	 * @throws IllegalArgumentException		if {@code minBound} == {@code null} or {@code maxBound} == {@code null}.
	 * @throws IllegalArgumentException		if {@code maxBound} < {@code minBound}.
	 */
	public IntervalTimeFilter(Instant minBound, Instant maxBound) {
        if (minBound == null || maxBound == null) throw new IllegalArgumentException("Bounds cannot be null");
        if (maxBound.isBefore(minBound)) throw new IllegalArgumentException("maxBound must be greater or equal to minBound");
		this.minBound = minBound;
		this.maxBound = maxBound;
	}
	
	@Override
	public boolean match(Instant timestamp) {
		if (timestamp == null) return false;
		return !timestamp.isBefore(minBound) && !timestamp.isAfter(maxBound); // [minBound, maxBound]
	}

}