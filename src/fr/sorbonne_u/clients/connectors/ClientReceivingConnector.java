package fr.sorbonne_u.clients.connectors;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;

/**
 * @author Kevin TANG
 * @author David VADIMON
 * 
 **/

public class ClientReceivingConnector extends AbstractConnector implements ReceivingCI {

	@Override
	public void receive(String channel, MessageI message) throws Exception {
		((ReceivingCI)this.offering).receive(channel, message);
		
	}

	@Override
	public void receive(String channel, MessageI[] messages) throws Exception {
		((ReceivingCI)this.offering).receive(channel, messages);
		
	}

}
