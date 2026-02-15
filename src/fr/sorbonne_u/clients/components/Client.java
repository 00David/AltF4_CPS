package fr.sorbonne_u.clients.components;

import java.util.stream.Stream;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.clients.connectors.ClientRegistrationServiceConnector;
import fr.sorbonne_u.clients.ports.ClientPublishingOutboundPort;
import fr.sorbonne_u.clients.ports.ClientReceivingInboundPort;
import fr.sorbonne_u.clients.ports.ClientRegistrationOutboundPort;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;

/**
 * La classe abstraite <code>Client</code> donne un composant qui est client du systeme publication/souscription.
 * @author Kevin TANG
 * @author David VADIMON
 */
@RequiredInterfaces(required= {RegistrationCI.class,PublishingCI.class})
@OfferedInterfaces(offered= {ReceivingCI.class})
public abstract class Client extends AbstractComponent {
	
	protected ClientRegistrationOutboundPort registrationOutboundPort;
	protected ClientPublishingOutboundPort publishingOutboundPort;
	protected ClientReceivingInboundPort receivingInboundPort;
	
	protected String brokerPublishingPortURI = null; // Si != null, ça veut dire que l'on s'est connecté au port de publication
	
	//--------------------------------------------------------------------------
	// Constructor
	//--------------------------------------------------------------------------
	
	/**
	 * Crée un composant client du système de publication/souscription.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code registrationOutboundPortURI != null && !registrationOutboundPortURI.isEmpty()}
	 * pre	{@code publishingOutboundPortURI != null && !publishingOutboundPortURI.isEmpty()}
	 * pre	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param registrationOutboundPortURI L'URI du port sortant réquérant {@link RegistrationCI}.
	 * @param publishingOutboundPortURI   L'URI du port sortant réquérant {@link PublishingCI}.
	 * @param receivingInboundPortURI     L'URI du port entrant offrant {@link ReceivingCI}.
	 * @throws Exception                  Si problème lors de l'initialisation ou de la publication des ports.
	 */
	protected Client(String registrationOutboundPortURI,String publishingOutboundPortURI,String receivingInboundPortURI) throws Exception {
		super(1,0);
		assert(registrationOutboundPortURI != null && !registrationOutboundPortURI.isEmpty());
		assert(publishingOutboundPortURI != null && !publishingOutboundPortURI.isEmpty());
		assert(receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty());
		
		//initializing outbound and inbound ports
		this.registrationOutboundPort = new ClientRegistrationOutboundPort(registrationOutboundPortURI, this);
		this.publishingOutboundPort = new ClientPublishingOutboundPort(publishingOutboundPortURI, this);
		this.receivingInboundPort = new ClientReceivingInboundPort(receivingInboundPortURI, this);
		
		//publish ports
		this.registrationOutboundPort.localPublishPort();
		this.publishingOutboundPort.localPublishPort();
		this.receivingInboundPort.publishPort();

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
	public synchronized void finalise() throws Exception{
		this.logMessage("stopping client component");
		
		// On déconnecte le port d'enregistrement
		this.doPortDisconnection(registrationOutboundPort.getPortURI());
		
		// On déconnecte le port de publication, si l'on s'est connecté
		if (brokerPublishingPortURI != null) this.doPortDisconnection(publishingOutboundPort.getPortURI());
		
		super.finalise();
	}
	
	@Override
	public synchronized void shutdown() throws ComponentShutdownException {
		
		try {
			// On unpublish les ports
			this.registrationOutboundPort.unpublishPort();
			this.receivingInboundPort.unpublishPort();
			this.publishingOutboundPort.unpublishPort();
		} catch (Throwable e) {
			throw new ComponentShutdownException(e);
		}
		
		super.shutdown();
	}

	//--------------------------------------------------------------------------
	// Component internal services
	//--------------------------------------------------------------------------
	
	/**
	 * Service permettant de traiter la réception d'un {@code message} depuis un {@code channel} où le composant est abonné.
	 * 
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code message != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param channel   Le canal depuis lequel le {@code message} a été reçu.
	 * @param message   Le message reçu depuis le {@code channel}.
	 * @return          null
	 */
	public Void receiveService(String channel, MessageI message) {
		// Vérification des préconditions
		assert(channel != null);
		assert(!channel.isEmpty());
		assert(message != null);
		
		this.logMessage("Client received message '" + message + "' from the channel : " + channel);
		return null;
	}
	
	/**
	 * Service permettant de traiter la réception de plusieurs {@code messages} depuis un {@code channel} où le composant est abonné.
	 * 
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code messages != null && messages.length > 0}
	 * pre	{@code Stream.of(messages).allMatch(m -> m != null)}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param channel   Le canal depuis lequel les {@code messages} ont été reçu.
	 * @param messages  Les messages reçus depuis le {@code channel}.
	 * @return          null
	 */
	public Void receiveService(String channel, MessageI[] messages) {
		// Vérification des préconditions
		assert(channel != null);
		assert(!channel.isEmpty());
		assert(messages != null);
		assert(messages.length > 0);
		assert(Stream.of(messages).allMatch(m -> m != null));
		
		this.logMessage("Client received multiple messages from the channel : " + channel);
		int i = 1;
		for (MessageI msg : messages) {
			this.logMessage("Message " + i + " : " + msg);
			i++;
		}
		return null;
	}
}
