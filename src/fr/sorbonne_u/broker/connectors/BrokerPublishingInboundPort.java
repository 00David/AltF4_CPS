package fr.sorbonne_u.broker.connectors;

import java.util.ArrayList;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;

/**
 * @author Kevin Tang
 * @author David Vadimon
 */
public class BrokerPublishingInboundPort
extends		AbstractInboundPort
implements	PublishingCI {

	private static final long serialVersionUID = 1878031196993861332L;

	public BrokerPublishingInboundPort(String uri, ComponentI owner) throws Exception {
		super(uri, PublishingCI.class, owner) ;
		assert	uri != null && owner instanceof Broker ;
	}

	@Override
	public void publish(String receptionPortURI, String channel, MessageI message) throws Exception {
		this.getOwner().handleRequest(
				owner -> ((Broker)owner).publish(receptionPortURI, channel, message)) ;
	}

	@Override
	public void publish(String receptionPortURI, String channel, ArrayList<MessageI> messages) throws Exception {
		this.getOwner().handleRequest(
				owner -> ((Broker)owner).publish(receptionPortURI, channel, messages)) ;
	}
	
}
