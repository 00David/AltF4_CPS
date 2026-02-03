package fr.sorbonne_u.broker.connectors;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;

/**
 * Connecteur entre un port sortant du composant {@link Broker} et un port entrant d'un composant client.<br>
 * Délègue la réception du message au port du composant client. 
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class ReceivingServiceConnector 
extends		AbstractConnector
implements	ReceivingCI {

	@Override
	public void receive(String channel, MessageI message) throws Exception {
		((ReceivingCI)this.offering).receive(channel, message);
	}

	@Override
	public void receive(String channel, MessageI[] messages) throws Exception {
		((ReceivingCI)this.offering).receive(channel, messages);
	}

}
