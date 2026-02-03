package fr.sorbonne_u.clients.connectors;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;

/**
 * @author Kevin TANG
 * @author David VADIMON
 * 
 **/

/**
 * La classe <code>ClientRegistrationConnector</code> implemente un connecteur pour le port 
 * <code>ClientRegistrationOutboundPort</code> pour lier le port <code>Registration/code> du <code>Broker</code>.
 */
public class ClientRegistrationConnector extends AbstractConnector implements RegistrationCI {

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
	public boolean channelExists(String channel) throws Exception {
		return ((RegistrationCI)this.offering).channelExists(channel);
	}

	@Override
	public boolean subscribed(String receptionPortURI, String channel) throws Exception {
		return ((RegistrationCI)this.offering).channelExists(channel);
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
