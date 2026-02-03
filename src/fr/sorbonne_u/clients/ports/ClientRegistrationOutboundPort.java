package fr.sorbonne_u.clients.ports;


import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;

/**
 * @author Kevin TANG
 * @author David VADIMON
 * 
 **/


/**
 * La classe <code>ClientRegistrationOutboundPort</code> implemente le port sortant 
 * pour l'enregistrement du <code>Client</code> sur le canal de messages.
 */
public class ClientRegistrationOutboundPort extends AbstractOutboundPort implements RegistrationCI {
	
	private static final long serialVersionUID = 1L;

	public ClientRegistrationOutboundPort(String uri,ComponentI owner) throws Exception {
		super(uri,RegistrationCI.class,owner);
	}
	
	public ClientRegistrationOutboundPort(ComponentI owner) throws Exception{
		super(RegistrationCI.class,owner);
		
		assert owner != null;
	}

	@Override
	public boolean registered(String receptionPortURI) throws Exception {
		return ((RegistrationCI)this.getConnector()).registered(receptionPortURI);
	}

	@Override
	public boolean registered(String receptionPortURI, RegistrationClass rc) throws Exception {
		return ((RegistrationCI)this.getConnector()).registered(receptionPortURI, rc);
	}

	@Override
	public String register(String receptionPortURI, RegistrationClass rc) throws Exception {
		return ((RegistrationCI)this.getConnector()).register(receptionPortURI, rc);
	}

	@Override
	public String modifyServiceClass(String receptionPortURI, RegistrationClass rc) throws Exception {
		return ((RegistrationCI)this.getConnector()).modifyServiceClass(receptionPortURI, rc);
	}

	@Override
	public void unregister(String receptionPortURI) throws Exception {
		((RegistrationCI)this.getConnector()).unregister(receptionPortURI);
		
	}

	@Override
	public boolean channelExists(String channel) throws Exception {
		return ((RegistrationCI)this.getConnector()).channelExists(channel);
	}

	@Override
	public boolean subscribed(String receptionPortURI, String channel) throws Exception {
		return ((RegistrationCI)this.getConnector()).subscribed(receptionPortURI, channel);
	}

	@Override
	public void subscribe(String receptionPortURI, String channel, MessageFilterI filter) throws Exception {
		((RegistrationCI)this.getConnector()).subscribe(receptionPortURI, channel, filter);
		
	}

	@Override
	public void unsubscribe(String receptionPortURI, String channel) throws Exception {
		((RegistrationCI)this.getConnector()).unsubscribe(receptionPortURI, channel);
		
	}

	@Override
	public boolean modifyFilter(String receptionPortURI, String channel, MessageFilterI filter) throws Exception {
		return ((RegistrationCI)this.getConnector()).modifyFilter(receptionPortURI, channel, filter);
	}

	
}
