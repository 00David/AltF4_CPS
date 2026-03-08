package fr.sorbonne_u.broker.components.infos.exceptions;

import fr.sorbonne_u.broker.components.infos.ClientBrokerInfos;

/**
 * 
 * Thrown when the number of remaining channels for a FREE client would become strictly positive, inside {@link ClientBrokerInfos}.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true} // no more invariant
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class CannotIncreaseRemainingChannelsException extends RuntimeException{
	
	private static final long serialVersionUID = -4480131682075412158L;

	/**
	 * Creates a CannotIncreaseRemainingChannelsException with an error message.
	 * @param msg Error message
	 */
	public CannotIncreaseRemainingChannelsException(String msg) {
		super(msg);
	}
	
	/**
	 * Creates a CannotIncreaseRemainingChannelsException.
	 */
	public CannotIncreaseRemainingChannelsException() {
		super();
	}
}
