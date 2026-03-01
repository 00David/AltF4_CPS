package fr.sorbonne_u.filters.value;

import java.io.Serializable;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.ValueFilterI;

/**
 * Filter accepting any {@code value} given as a parameter to the {@code match} method.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class JokerValueFilter implements ValueFilterI{

	private static final long serialVersionUID = -7611950551672657716L;

	@Override
	public boolean match(Serializable value) {
		return true;
	}

}