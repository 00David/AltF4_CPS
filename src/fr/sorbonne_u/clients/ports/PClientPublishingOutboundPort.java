package fr.sorbonne_u.clients.ports;


import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;

public class PClientPublishingOutboundPort extends ClientPublishingOutboundPort implements PrivilegedClientCI {

	private static final long serialVersionUID = 1L;

	public PClientPublishingOutboundPort(ComponentI owner) throws Exception {
		super(owner);
	}

	public PClientPublishingOutboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, owner);
	}

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
	public boolean isAuthorisedUser(String channel, String uri) throws Exception {
		return ((PrivilegedClientCI)this.getConnector()).isAuthorisedUser(channel, uri);
	}

	@Override
	public void modifyAuthorisedUsers(String receptionPortURI, String channel, String autorisedUsers) throws Exception {
		((PrivilegedClientCI)this.getConnector()).modifyAuthorisedUsers(receptionPortURI, channel, autorisedUsers);
	}

	@Override
	public void removeAuthorisedUsers(String receptionPortURI, String channel, String regularExpression)
			throws Exception {
		((PrivilegedClientCI)this.getConnector()).removeAuthorisedUsers(receptionPortURI, channel, regularExpression);
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
