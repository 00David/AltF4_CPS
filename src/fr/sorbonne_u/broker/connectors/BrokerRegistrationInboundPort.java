package fr.sorbonne_u.broker.connectors;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;

/**
 * @author Kevin Tang
 * @author David Vadimon
 */
public class BrokerRegistrationInboundPort
extends		AbstractInboundPort
implements	RegistrationCI {

	private static final long serialVersionUID = 763740080899636307L;

	public BrokerRegistrationInboundPort(String uri, ComponentI owner) throws Exception {
			super(uri, RegistrationCI.class, owner) ;
			assert	uri != null && owner instanceof Broker ;
	}

	@Override
	public boolean registered(String receptionPortURI) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((Broker)owner).registered(receptionPortURI)) ;
	}

	@Override
	public boolean registered(String receptionPortURI, RegistrationClass rc) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((Broker)owner).registered(receptionPortURI, rc)) ;
	}

	@Override
	public String register(String receptionPortURI, RegistrationClass rc) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((Broker)owner).register(receptionPortURI, rc));
	}

	@Override
	public String modifyServiceClass(String receptionPortURI, RegistrationClass rc) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((Broker)owner).modifyServiceClass(receptionPortURI, rc));
	}

	@Override
	public void unregister(String receptionPortURI) throws Exception {
		this.getOwner().handleRequest(
				owner -> ((Broker)owner).unregister(receptionPortURI));
	}

	@Override
	public boolean channelExists(String channel) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((Broker)owner).channelExists(channel));
	}

	@Override
	public boolean subscribed(String receptionPortURI, String channel) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((Broker)owner).subscribed(receptionPortURI, channel));
	}

	@Override
	public void subscribe(String receptionPortURI, String channel, MessageFilterI filter) throws Exception {
		this.getOwner().handleRequest(
				owner -> ((Broker)owner).subscribe(receptionPortURI, channel, filter));
	}

	@Override
	public void unsubscribe(String receptionPortURI, String channel) throws Exception {
		this.getOwner().handleRequest(
				owner -> ((Broker)owner).unsubscribe(receptionPortURI, channel));
	}

	@Override
	public boolean modifyFilter(String receptionPortURI, String channel, MessageFilterI filter) throws Exception {
		return this.getOwner().handleRequest(
				owner -> ((Broker)owner).modifyFilter(receptionPortURI, channel, filter));
	}
	
}
