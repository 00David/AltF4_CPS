package fr.sorbonne_u.filters.time;

import java.time.Instant;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.TimeFilterI;

/**
 * Filtre temporel acceptant n'importe quel {@code timestamp} en paramètre de la méthode {@code match}.<br>
 * Le {@code timestamp} peut être {@code null}.
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class JokerTimeFilter implements TimeFilterI {

	private static final long serialVersionUID = -7157528558310994997L;

	/**
	 * @throws IllegalArgumentException si {@code timestamp} == {@code null}.
	 */
	@Override
	public boolean match(Instant timestamp) {
		// Verif précondition
		if (timestamp == null) throw new IllegalArgumentException("Timestamp cannot be null");
		return true;
	}
}
