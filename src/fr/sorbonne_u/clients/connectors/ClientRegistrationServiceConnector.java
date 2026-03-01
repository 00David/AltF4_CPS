package fr.sorbonne_u.clients.connectors;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;

/**
 * Connector between an outbound port of the {@link Client} component and an inbound port of a {@link Broker}.<br>
 * Delegates registration to the {@link BrokerRegistrationInboundPort}
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class ClientRegistrationServiceConnector extends AbstractConnector implements RegistrationCI {

	// Precondition and postcondition verifications are delegated to the the BrokerRegistrationInboundPort
	
	@Override
	public boolean registered(String receptionPortURI) throws Exception {
		return ((RegistrationCI)this.offering).registered(receptionPortURI);
	}

	@Override
	public boolean registered(String receptionPortURI, RegistrationClass rc) throws Exception {
		return ((RegistrationCI)this.offering).registered(receptionPortURI, rc);
	}

	@Override
	public String register(String receptionPortURI, RegistrationClass rc) throws Exception {
		return ((RegistrationCI)this.offering).register(receptionPortURI, rc);
	}

	@Override
	public String modifyServiceClass(String receptionPortURI, RegistrationClass rc) throws Exception {
		return ((RegistrationCI)this.offering).modifyServiceClass(receptionPortURI, rc);
	}

	@Override
	public void unregister(String receptionPortURI) throws Exception {
		((RegistrationCI)this.offering).unregister(receptionPortURI);
		
	}

	@Override
	public boolean channelExist(String channel) throws Exception {
		return ((RegistrationCI)this.offering).channelExist(channel);
	}
	
	@Override
	public boolean channelAuthorised(String receptionPortURI, String channel) throws Exception {
		return ((RegistrationCI)this.offering).channelAuthorised(receptionPortURI, channel);
	}

	@Override
	public boolean subscribed(String receptionPortURI, String channel) throws Exception {
		return ((RegistrationCI)this.offering).channelExist(channel);
	}

	@Override
	public void subscribe(String receptionPortURI, String channel, MessageFilterI filter) throws Exception {
		((RegistrationCI)this.offering).subscribe(receptionPortURI, channel, filter);
		
	}

	@Override
	public void unsubscribe(String receptionPortURI, String channel) throws Exception {
		((RegistrationCI)this.offering).unsubscribe(receptionPortURI, channel);
		
	}

	@Override
	public boolean modifyFilter(String receptionPortURI, String channel, MessageFilterI filter) throws Exception {
		return ((RegistrationCI)this.offering).modifyFilter(receptionPortURI, channel, filter);
	}
}