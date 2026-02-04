package fr.sorbonne_u.clients.ports;

import java.util.ArrayList;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;

/**
 * La classe <code>ClientPublishingOutboundPort</code> implement un port sortant pour la publication de messages.
 * @author Kevin TANG
 * @author David VADIMON
 */
public class ClientPublishingOutboundPort extends AbstractOutboundPort implements PublishingCI{

	private static final long serialVersionUID = 1L;

	public ClientPublishingOutboundPort(String uri, ComponentI owner) throws Exception {
		super(uri,PublishingCI.class,owner);
		assert	uri != null && owner != null ;
	}
	
	public ClientPublishingOutboundPort(ComponentI owner) throws Exception{
		super(PublishingCI.class,owner);
		assert	owner != null ;
	}

	@Override
	public void publish(String receptionPortURI, String channel, MessageI message) throws Exception {
		((PublishingCI)this.getConnector()).publish(receptionPortURI, channel, message);
		
	}

	@Override
	public void publish(String receptionPortURI, String channel, ArrayList<MessageI> messages) throws Exception {
		((PublishingCI)this.getConnector()).publish(receptionPortURI, channel, messages);
		
	}
}
