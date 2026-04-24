package fr.sorbonne_u.clients.connectors;

import java.util.ArrayList;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;
import fr.sorbonne_u.clients.ports.PrivilegedOutboundPort;

/**
 * Connector between a {@link PrivilegedOutboundPort} and a port implementing {@link PrivilegedClientCI}, 
 * delegates privileged management to this port.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class PrivilegedServiceConnector extends PublishingServiceConnector implements PrivilegedClientCI {

	// Precondition and postcondition verifications are delegated to the the port implementing PrivilegedClientCI
	
	@Override
	public void publish(String receptionPortURI, String channel, MessageI message) throws Exception {
		((PrivilegedClientCI)this.offering).publish(receptionPortURI, channel, message);
	}

	@Override
	public void publish(String receptionPortURI, String channel, ArrayList<MessageI> messages) throws Exception {
		((PrivilegedClientCI)this.offering).publish(receptionPortURI, channel, messages);
	}
	
	@Override
	public boolean hasCreatedChannel(String receptionPortURI, String channel) throws Exception {
		return ((PrivilegedClientCI)this.offering).hasCreatedChannel(receptionPortURI, channel);
	}

	@Override
	public boolean channelQuotaReached(String receptionPortURI) throws Exception {
		return ((PrivilegedClientCI)this.offering).channelQuotaReached(receptionPortURI);
	}

	@Override
	public void createChannel(String receptionPortURI, String channel, String autorisedUsers) throws Exception {
		((PrivilegedClientCI)this.offering).createChannel(receptionPortURI, channel, autorisedUsers);
	}

	@Override
	public void modifyAuthorisedUsers(String receptionPortURI, String channel, String autorisedUsers) throws Exception {
		((PrivilegedClientCI)this.offering).modifyAuthorisedUsers(receptionPortURI, channel, autorisedUsers);
	}

	@Override
	public void destroyChannel(String receptionPortURI, String channel) throws Exception {
		((PrivilegedClientCI)this.offering).destroyChannel(receptionPortURI, channel);
		
	}

	@Override
	public void destroyChannelNow(String receptionPortURI, String channel) throws Exception {
		((PrivilegedClientCI)this.offering).destroyChannelNow(receptionPortURI, channel);
		
	}
}