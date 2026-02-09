package fr.sorbonne_u.filters.time;

import java.time.Instant;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.TimeFilterI;

/**
 * Filtre temporel gardant les {@code timestamp} (de la méthode {@code match}) >= {@code timestampFilter} (du constructeur).
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class GreaterOrEqualTimeFilter implements TimeFilterI{

	private static final long serialVersionUID = -7998024328704643904L;
	private Instant timestampFilter;
	
	/**
	 * Construit le filtre, avec le {@code timestampFilter} utilisé dans le {@code match}.
	 * @param timestampFilter Borne temporelle inférieure du filtre.
	 * @throws IllegalArgumentException si {@code timestampFilter} == {@code null}.
	 */
	public GreaterOrEqualTimeFilter(Instant timestampFilter) {
        if (timestampFilter == null) throw new IllegalArgumentException("Timestamp cannot be null");
		this.timestampFilter = timestampFilter;
	}
	
	/**
	 * @throws IllegalArgumentException si {@code timestamp} == {@code null}.
	 */
	@Override
	public boolean match(Instant timestamp) {
		// Verif précondition
		if (timestamp == null) throw new IllegalArgumentException("Timestamp cannot be null");
	    return !timestamp.isBefore(timestampFilter); // >=
	}

}
