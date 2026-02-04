package fr.sorbonne_u.clients.components;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.clients.connectors.ClientPublishingServiceConnector;
import fr.sorbonne_u.clients.connectors.ClientRegistrationServiceConnector;
import fr.sorbonne_u.clients.ports.ClientPublishingOutboundPort;
import fr.sorbonne_u.clients.ports.ClientReceivingInboundPort;
import fr.sorbonne_u.clients.ports.ClientRegistrationOutboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertiesFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertyFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.filters.MessageFilter;
import fr.sorbonne_u.messages.Message;

/**
 * 
 * La classe <code>Client</code> implemente un composant qui est client du systeme publication/souscription
 * @author Kevin TANG
 * @author David VADIMON
 */


@RequiredInterfaces(required= {RegistrationCI.class,PublishingCI.class})
@OfferedInterfaces(offered= {ReceivingCI.class})


public class Client extends AbstractComponent {
	
	protected ClientRegistrationOutboundPort registrationOutboundPort;
	protected ClientPublishingOutboundPort publishingOutboundPort;
	protected ClientReceivingInboundPort receivingInboundPort;
	
	private String brokerPublishingPortURI = null; // Si != null, ça veut dire que l'on s'est connecté au port de publication
	
	//--------------------------------------------------------------------------
	// Constructor
	//--------------------------------------------------------------------------
	
	protected Client(String registrationOutboundPortURI,String publishingOutboundPortURI,String receivingInboundPortURI) throws Exception {
		super(1,0);
		
		//initializing outbound and inbound ports
		this.registrationOutboundPort = new ClientRegistrationOutboundPort(registrationOutboundPortURI, this);
		this.publishingOutboundPort = new ClientPublishingOutboundPort(publishingOutboundPortURI, this);
		this.receivingInboundPort = new ClientReceivingInboundPort(receivingInboundPortURI, this);
		
		//publish ports
		this.registrationOutboundPort.localPublishPort();
		this.publishingOutboundPort.localPublishPort();
		this.receivingInboundPort.publishPort();
		
		this.getTracer().setTitle("Client") ;
		this.getTracer().setRelativePosition(1, 1) ;

		AbstractComponent.checkImplementationInvariant(this);
		AbstractComponent.checkInvariant(this);
		
	}
	
	//--------------------------------------------------------------------------
	// Component life-cycle
	//--------------------------------------------------------------------------
	
	@Override
	public synchronized void start() throws ComponentStartException {
		this.logMessage("starting client component");
		
		// On connecte le port d'enregistrement
		try {
			this.doPortConnection(this.registrationOutboundPort.getPortURI(),
					  Broker.registrationPortURI(),
					  ClientRegistrationServiceConnector.class.getCanonicalName());
		} catch (Throwable e) {
			throw new ComponentStartException(e) ;
		}
		
		super.start();
	}
		
	
	@Override
	public void execute() throws Exception{
		super.execute();
		this.logMessage("executing client component");
		
		this.runTask(
				new AbstractComponent.AbstractTask() {
					@Override
					public void run() {
						try {
							((Client)this.getTaskOwner()).exampleSendMessage() ;
						} catch (Throwable e) {
							e.printStackTrace();
						}
					}
				}) ;
	}
	
	
	@Override
	public synchronized void finalise() throws Exception{
		this.logMessage("stopping client component");
		
		// On déconnecte le port d'enregistrement
		this.doPortDisconnection(registrationOutboundPort.getPortURI());
		
		// On déconnecte le port de publication, si l'on s'est connecté
		if (brokerPublishingPortURI != null) this.doPortDisconnection(publishingOutboundPort.getPortURI());
		
		// On unpublish les ports
		this.registrationOutboundPort.unpublishPort();
		this.receivingInboundPort.unpublishPort();
		this.publishingOutboundPort.unpublishPort();
		
		super.finalise();
	}
	
	//--------------------------------------------------------------------------
	// Exemple d'execution d'un client
	//--------------------------------------------------------------------------
	
	public void exampleSendMessage() throws Exception {
		if(!this.registrationOutboundPort.registered(receivingInboundPort.getPortURI())) {
			// On s'enregistre et on récupère l'URI du port entrant de publication du Broker
			brokerPublishingPortURI = this.registrationOutboundPort.register(receivingInboundPort.getPortURI(), RegistrationClass.FREE);
			
			// On connecte le port de publication
			this.doPortConnection(this.publishingOutboundPort.getPortURI(),
					brokerPublishingPortURI,
					ClientPublishingServiceConnector.class.getCanonicalName());
			
			// On s'enregistre sur un channel existant
			this.registrationOutboundPort.subscribe(
					receivingInboundPort.getPortURI(), 
					"Channel1", 
					new MessageFilter(new PropertyFilterI[0], new PropertiesFilterI[0], null)); // Filtre vide
			
			// On envoie un message au channel
			Message msg = new Message();
			msg.setPayload("Voici un message");
			this.publishingOutboundPort.publish(receivingInboundPort.getPortURI(), "Channel1", msg);
		}
	}

	//--------------------------------------------------------------------------
	// Component internal services
	//--------------------------------------------------------------------------
	
	public Void receiveService(String channel, MessageI message) {
		this.logMessage("Client received message '" + message + "' from the channel : " + channel);
		return null;
	}
	
	public Void receiveService(String channel, MessageI[] messages) {
		this.logMessage("Client received multiple messages from the channel : " + channel);
		int i = 1;
		for (MessageI msg : messages) {
			this.logMessage("Message " + i + " : " + msg);
			i++;
		}
		return null;
	}
}
