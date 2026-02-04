package fr.sorbonne_u.clients.connectors;

import java.util.ArrayList;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;

/**
 * La classe <code>ClientPublishingServiceConnector</code> implemente un connecteur pour le port 
 * <code>ClientPublishingOutboundPort</code> pour lier le port <code>Publishing</code> du <code>Broker</code>.
 * @author Kevin TANG
 * @author David VADIMON
 */
public class ClientPublishingServiceConnector extends AbstractConnector implements PublishingCI{

	
	@Override
	public void publish(String receptionPortURI, String channel, MessageI message) throws Exception {
		((PublishingCI)this.offering).publish(receptionPortURI, channel, message);
		
	}

	@Override
	public void publish(String receptionPortURI, String channel, ArrayList<MessageI> messages) throws Exception {
		((PublishingCI)this.offering).publish(receptionPortURI, channel, messages);
		
	}

	

}
