package fr.sorbonne_u.filters.time;

import java.time.Instant;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.TimeFilterI;

/**
 * Filtre temporel gardant les {@code timestamp} (de la méthode {@code match}) vérifiant : <br>
 * {@code minBound} <= {@code timestamp} <= {@code maxBound}, avec {@code minBound} et {@code maxBound} fixés dans le constructeur.
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class IntervalTimeFilter implements TimeFilterI{

	private static final long serialVersionUID = -1021372401821067437L;
	private Instant minBound;
	private Instant maxBound;
	
	/**
	 * Construit le filtre, avec les {@code minBound} et {@code maxBound} utilisés dans le {@code match}.
	 * @param minBound Borne temporelle inférieure du filtre.
	 * @param maxBound Borne temporelle supérieure du filtre.
	 * @throws IllegalArgumentException si {@code minBound} == {@code null} ou {@code maxBound} == {@code null}.
	 * @throws IllegalArgumentException si {@code maxBound} < {@code minBound}.
	 */
	public IntervalTimeFilter(Instant minBound, Instant maxBound) {
        if (minBound == null || maxBound == null) throw new IllegalArgumentException("Bounds cannot be null");
        if (maxBound.isBefore(minBound)) throw new IllegalArgumentException("maxBound must be greater or equal to minBound");
		this.minBound = minBound;
		this.maxBound = maxBound;
	}
	
	/**
	 * @throws IllegalArgumentException si {@code timestamp} == {@code null}.
	 */
	@Override
	public boolean match(Instant timestamp) {
		// Verif précondition
		if (timestamp == null) throw new IllegalArgumentException("Timestamp cannot be null");
		return !timestamp.isBefore(minBound) && !timestamp.isAfter(maxBound); // [minBound, maxBound]
	}

}
