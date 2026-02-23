package fr.sorbonne_u.clients.components;

import fr.sorbonne_u.clients.ports.ClientPublishingOutboundPort;
import fr.sorbonne_u.clients.ports.ClientReceivingInboundPort;
import fr.sorbonne_u.clients.ports.ClientRegistrationOutboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.PluginI;
import fr.sorbonne_u.components.exceptions.PluginException;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * La classe abstraite donnant un composant qui est client du système de publication/souscription, utilisant des plugins.
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public abstract class ClientWithPlugin  extends AbstractComponent {
	
	protected final String registrationOutboundPortURI;
	protected final String publishingOutboundPortURI;
	protected final String receivingInboundPortURI;
	
	protected ClientWithPlugin(String registrationOutboundPortURI,String publishingOutboundPortURI,String receivingInboundPortURI) throws Exception {
		super(1,0);
		assert(registrationOutboundPortURI != null && !registrationOutboundPortURI.isEmpty());
		assert(publishingOutboundPortURI != null && !publishingOutboundPortURI.isEmpty());
		assert(receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty());
		
		this.registrationOutboundPortURI = registrationOutboundPortURI;
		this.publishingOutboundPortURI = publishingOutboundPortURI;
		this.receivingInboundPortURI = receivingInboundPortURI;
		
	}
	
}
