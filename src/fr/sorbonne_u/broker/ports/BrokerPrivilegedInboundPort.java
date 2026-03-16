package fr.sorbonne_u.broker.ports;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * Inbound port of a {@link Broker}, allowing the handling of message publications from a privileged client, 
 * as well as advanced channel management operations.<br>
 * Delegates the processing to its owner component, a {@link Broker}.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class BrokerPrivilegedInboundPort
extends		BrokerPublishingInboundPort
implements	PrivilegedClientCI {

	private static final long serialVersionUID = 1878031196993861332L;

	/**
	 * Creates the inbound port with the given {@code uri}, whose owner
	 * component {@code owner} is a {@link Broker}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre {@code uri != null && !uri.isEmpty()}
	 * pre {@code owner != null}
	 * pre {@code owner instanceof Broker}
	 * post true            // no postcondition.
	 * </pre>
	 * 
	 * @param uri    		the URI of the port.
	 * @param owner  		the component owning the port.
	 * @throws Exception 	if an error occurs while creating the port
	 */
	public BrokerPrivilegedInboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, PrivilegedClientCI.class, owner) ;
		// Precondition checks
		assert(uri != null && !uri.isEmpty()) : new PreconditionException("port uri cannot be null or empty");
		assert(owner != null) : new PreconditionException("owner cannot be null");
		assert(owner instanceof Broker) : new PreconditionException("owner must be a Broker");
	}
	
	// Precondition and postcondition verifications are delegated to the Broker

	@Override
	public boolean hasCreatedChannel(String receptionPortURI, String channel) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((Broker)owner).hasCreatedChannelService(receptionPortURI, channel));
	}

	@Override
	public boolean channelQuotaReached(String receptionPortURI) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((Broker)owner).channelQuotaReachedService(receptionPortURI));
	}

	@Override
	public void createChannel(String receptionPortURI, String channel, String autorisedUsers) throws Exception {
		this.getOwner().handleRequest(
				owner -> ((Broker)owner).createChannelService(receptionPortURI, channel, autorisedUsers));
	}

	@Override
	public boolean isAuthorisedUser(String channel, String uri) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((Broker)owner).isAuthorisedUserService(channel, uri));
	}

	@Override
	public void modifyAuthorisedUsers(String receptionPortURI, String channel, String autorisedUsers) throws Exception {
		this.getOwner().handleRequest(
				owner -> ((Broker)owner).modifyAuthorisedUsersService(receptionPortURI, channel, autorisedUsers));
	}

	// 'removeAuthorisedUsers' removed from source interface
	@Override
	public void removeAuthorisedUsers(String receptionPortURI, String channel, String regularExpression)
			throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void destroyChannel(String receptionPortURI, String channel) throws Exception {
		this.getOwner().handleRequest(
				owner -> ((Broker)owner).destroyChannelService(receptionPortURI, channel));
	}

	@Override
	public void destroyChannelNow(String receptionPortURI, String channel) throws Exception {
		this.getOwner().handleRequest(
				owner -> ((Broker)owner).destroyChannelNowService(receptionPortURI, channel));
	}
	
}
