package fr.sorbonne_u.clients.plugins;

import java.time.Duration;
import java.util.concurrent.Future;

import fr.sorbonne_u.clients.ports.ClientRegistrationOutboundPort;
import fr.sorbonne_u.components.AbstractPlugin;
import fr.sorbonne_u.cps.pubsub.exceptions.NotSubscribedChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnauthorisedClientException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownClientException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.plugins.interfaces.ClientRegistrationI;
import fr.sorbonne_u.cps.pubsub.plugins.interfaces.ClientSubscriptionI;

public class ClientSubscriptionPlugin extends AbstractPlugin implements ClientSubscriptionI {

	private static final long serialVersionUID = -1472841763925529259L;

	//protected final String receivingInboundPortURI;
	
	
	@Override
	public boolean channelExist(String channel) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean channelAuthorised(String channel) throws UnknownClientException, UnknownChannelException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean subscribed(String channel) throws UnknownClientException, UnknownChannelException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void subscribe(String channel, MessageFilterI filter)
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void unsubscribe(String channel) throws UnknownClientException, UnknownChannelException,
			UnauthorisedClientException, NotSubscribedChannelException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void modifyFilter(String channel, MessageFilterI filter) throws UnknownClientException,
			UnknownChannelException, UnauthorisedClientException, NotSubscribedChannelException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void receive(String channel, MessageI message) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void receive(String channel, MessageI[] messages) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public MessageI waitForNextMessage(String channel) throws UnknownClientException, UnknownChannelException,
			UnauthorisedClientException, NotSubscribedChannelException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MessageI waitForNextMessage(String channel, Duration d) throws UnknownClientException,
			UnknownChannelException, UnauthorisedClientException, NotSubscribedChannelException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Future<MessageI> getNextMessage(String channel) throws UnknownClientException, UnknownChannelException,
			UnauthorisedClientException, NotSubscribedChannelException {
		// TODO Auto-generated method stub
		return null;
	}

}
