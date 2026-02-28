package fr.sorbonne_u.filters.time;

import java.time.Instant;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.TimeFilterI;

/**
 * Time filter accepting any {@code timestamp} as a parameter of the {@code match} method.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class JokerTimeFilter implements TimeFilterI {

	private static final long serialVersionUID = -7157528558310994997L;

	@Override
	public boolean match(Instant timestamp) {
		return true;
	}
}