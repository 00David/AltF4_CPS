package fr.sorbonne_u.broker.connectors;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.clients.ports.ClientReceivingPluginInboundPort;
import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;

/**
 * Connector between an outbound port of the {@link Broker} component and an inbound port of a client component.<br>
 * Delegates message reception to the {@link ClientReceivingPluginInboundPort}
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class BrokerReceivingServiceConnector 
extends		AbstractConnector
implements	ReceivingCI {

	// Precondition and postconditions verifications are delegated to the the ClientReceivingPluginInboundPort
	
	@Override
	public void receive(String channel, MessageI message) throws Exception {
		((ReceivingCI)this.offering).receive(channel, message);
	}

	@Override
	public void receive(String channel, MessageI[] messages) throws Exception {
		((ReceivingCI)this.offering).receive(channel, messages);
	}

}
