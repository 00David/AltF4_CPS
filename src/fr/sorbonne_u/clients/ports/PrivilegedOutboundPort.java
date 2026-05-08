package fr.sorbonne_u.clients.ports;

import fr.sorbonne_u.broker.component.Broker;
import fr.sorbonne_u.clients.component.Client;
import fr.sorbonne_u.clients.connectors.PrivilegedServiceConnector;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * Delegates publication and privileged management to its {@link PrivilegedServiceConnector}.
 * 
 * <p>
 * Used as an outbound port of a {@link Client}, 
 * allowing the publication and privileged management to a {@link Broker}.
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class PrivilegedOutboundPort extends PublishingOutboundPort implements PrivilegedClientCI {
	
	private static final long serialVersionUID = 373413793763732617L;

	/**
	 * Creates the outbound port redirecting to a {@link PrivilegedServiceConnector}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre {@code owner != null}
	 * post {@code  true}	// no postcondition.
	 * </pre>
	 * 
	 * @param owner  		the component owning this port.
	 * @throws Exception 	if an error occurs while creating the port
	 */
	public PrivilegedOutboundPort(ComponentI owner) throws Exception {
		super(PrivilegedClientCI.class, owner);
		// Precondition check
		assert(owner != null) : new PreconditionException("owner cannot be null");
	}
	
	// Precondition and postcondition verifications are delegated to the PrivilegedServiceConnector

	@Override
	public boolean hasCreatedChannel(String receptionPortURI, String channel) throws Exception {
		return ((PrivilegedClientCI)this.getConnector()).hasCreatedChannel(receptionPortURI, channel);
	}

	@Override
	public boolean channelQuotaReached(String receptionPortURI) throws Exception {
		return ((PrivilegedClientCI)this.getConnector()).channelQuotaReached(receptionPortURI);
	}

	@Override
	public void createChannel(String receptionPortURI, String channel, String autorisedUsers) throws Exception {
		((PrivilegedClientCI)this.getConnector()).createChannel(receptionPortURI, channel, autorisedUsers);
		
	}

	@Override
	public void modifyAuthorisedUsers(String receptionPortURI, String channel, String autorisedUsers) throws Exception {
		((PrivilegedClientCI)this.getConnector()).modifyAuthorisedUsers(receptionPortURI, channel, autorisedUsers);
	}

	@Override
	public void destroyChannel(String receptionPortURI, String channel) throws Exception {
		((PrivilegedClientCI)this.getConnector()).destroyChannel(receptionPortURI, channel);
		
	}

	@Override
	public void destroyChannelNow(String receptionPortURI, String channel) throws Exception {
		((PrivilegedClientCI)this.getConnector()).destroyChannelNow(receptionPortURI, channel);
		
	}
}