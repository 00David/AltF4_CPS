package fr.sorbonne_u.broker.components.infos.exceptions;

import fr.sorbonne_u.broker.components.infos.ClientBrokerInfos;

/**
 * 
 * Thrown when the number of channels for standard or premium clients has not yet been initialized, inside {@link ClientBrokerInfos}.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true} // no more invariant
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class ChannelNumberNotInitializedException extends RuntimeException{

	private static final long serialVersionUID = -2318387136529555611L;

	public ChannelNumberNotInitializedException(String msg) {
		super(msg);
	}
	
	public ChannelNumberNotInitializedException() {
		super();
	}
}
