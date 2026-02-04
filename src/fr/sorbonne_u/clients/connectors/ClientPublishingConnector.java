package fr.sorbonne_u.clients.connectors;

import java.util.ArrayList;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;

/**
 * @author Kevin TANG
 * @author David VADIMON
 * 
 **/


/**
 * La classe <code>ClientPublishingConnector</code> implemente un connecteur pour le port 
 * <code>ClientPublishingOutboundPort</code> pour lier le port <code>Publishing</code> du <code>Broker</code>.
 */
public class ClientPublishingConnector extends AbstractConnector implements PublishingCI{

	
	@Override
	public void publish(String receptionPortURI, String channel, MessageI message) throws Exception {
		((PublishingCI)this.offering).publish(receptionPortURI, channel, message);
		
	}

	@Override
	public void publish(String receptionPortURI, String channel, ArrayList<MessageI> messages) throws Exception {
		((PublishingCI)this.offering).publish(receptionPortURI, channel, messages);
		
	}

	

}
