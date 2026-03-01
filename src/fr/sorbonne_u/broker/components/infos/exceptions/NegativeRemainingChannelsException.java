package fr.sorbonne_u.broker.components.infos.exceptions;

import fr.sorbonne_u.broker.components.infos.ClientBrokerInfos;

/**
 * 
 * Thrown when the number of remaining channels for a client would become negative, inside {@link ClientBrokerInfos}.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true} // no more invariant
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class NegativeRemainingChannelsException extends RuntimeException{
	private static final long serialVersionUID = -6291686257911217022L;
	
	public NegativeRemainingChannelsException(String msg) {
		super(msg);
	}
	
	public NegativeRemainingChannelsException() {
		super();
	}
}
