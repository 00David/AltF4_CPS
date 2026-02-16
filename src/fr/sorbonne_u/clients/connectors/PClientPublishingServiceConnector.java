package fr.sorbonne_u.clients.connectors;

import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;

public class PClientPublishingServiceConnector extends ClientPublishingServiceConnector implements PrivilegedClientCI {

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
	public boolean isAuthorisedUser(String channel, String uri) throws Exception {
		return ((PrivilegedClientCI)this.offering).isAuthorisedUser(channel, uri);
	}

	@Override
	public void modifyAuthorisedUsers(String receptionPortURI, String channel, String autorisedUsers) throws Exception {
		((PrivilegedClientCI)this.offering).modifyAuthorisedUsers(receptionPortURI, channel, autorisedUsers);
	}

	@Override
	public void removeAuthorisedUsers(String receptionPortURI, String channel, String regularExpression)
			throws Exception {
		((PrivilegedClientCI)this.offering).removeAuthorisedUsers(receptionPortURI, channel, regularExpression);
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
