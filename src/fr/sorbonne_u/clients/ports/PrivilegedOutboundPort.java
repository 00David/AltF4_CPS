package fr.sorbonne_u.clients.ports;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.clients.components.Client;
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
	 * Creates the outbound port with the provided {@code uri} redirecting to a {@link PrivilegedServiceConnector}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre {@code uri != null && !uri.isEmpty()}
	 * pre {@code owner != null}
	 * post {@code  true}	// no postcondition.
	 * </pre>
	 * 
	 * @param uri    		the URI of the port.
	 * @param owner  		the component owning this port.
	 * @throws Exception 	if an error occurs while creating the port
	 */
	public PrivilegedOutboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, PrivilegedClientCI.class, owner);
		// Precondition checks
		assert(uri != null && !uri.isEmpty()) : new PreconditionException("port uri cannot be null or empty");
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