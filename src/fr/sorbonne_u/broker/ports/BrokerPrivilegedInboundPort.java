package fr.sorbonne_u.broker.ports;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.tasks_management.connection.AbnormalTerminationNotificationOutboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;

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
	 * pre  {@code uri != null && !uri.isEmpty()}
	 * pre  {@code owner != null}
	 * pre  {@code owner instanceof Broker}
	 * pre  {@code notificationPort != null}
	 * post class invariant satisfied (for the parent class)
	 * </pre>
	 * 
	 * @param uri    					the URI of the port.
	 * @param owner  					the component owning the port.
	 * @param notificationPort  		outport port requiring the component interface {@code AbnormalTerminationNotificationCI}.
	 * @throws Exception 				if an error occurs while creating the port
	 */
	public BrokerPrivilegedInboundPort(String uri, ComponentI owner, 
			AbnormalTerminationNotificationOutboundPort notificationPort) throws Exception {
		super(uri, PrivilegedClientCI.class, owner, notificationPort) ;
		// Preconditions checked in the parent constructor
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
				owner -> ((Broker)owner).createChannelService(receptionPortURI, channel, autorisedUsers, true));
	}

	@Override
	public void modifyAuthorisedUsers(String receptionPortURI, String channel, String autorisedUsers) throws Exception {
		this.getOwner().handleRequest(
				owner -> ((Broker)owner).modifyAuthorisedUsersService(receptionPortURI, channel, autorisedUsers, true));
	}

	@Override
	public void destroyChannel(String receptionPortURI, String channel) throws Exception {
		this.getOwner().handleRequest(
				owner -> ((Broker)owner).destroyChannelService(receptionPortURI, channel, true));
	}

	@Override
	public void destroyChannelNow(String receptionPortURI, String channel) throws Exception {
		this.getOwner().handleRequest(
				owner -> ((Broker)owner).destroyChannelNowService(receptionPortURI, channel, true));
	}
	
}
