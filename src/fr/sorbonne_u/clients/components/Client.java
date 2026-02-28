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
 * The abstract class providing a component that acts as a client of the publish/subscribe system.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code registrationOutboundPort != null}
 * invariant	{@code publishingOutboundPort != null}
 * invariant	{@code publishingOutboundPort != null}
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
@RequiredInterfaces(required= {RegistrationCI.class,PublishingCI.class})
@OfferedInterfaces(offered= {ReceivingCI.class})
public abstract class Client extends AbstractComponent {
	
	protected ClientRegistrationOutboundPort registrationOutboundPort;
	protected ClientPublishingOutboundPort publishingOutboundPort;
	protected ClientReceivingInboundPort receivingInboundPort;
	
	protected String brokerPublishingPortURI = null; // If != null, it means the client has connected to the Broker publishing port
	
	//--------------------------------------------------------------------------  
	// Constructor
	//--------------------------------------------------------------------------  
	
	/**
	 * Creates a client component of the publish/subscribe system.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code registrationOutboundPortURI != null && !registrationOutboundPortURI.isEmpty()}
	 * pre	{@code publishingOutboundPortURI != null && !publishingOutboundPortURI.isEmpty()}
	 * pre	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param registrationOutboundPortURI The URI of the outbound port requiring {@link RegistrationCI}.
	 * @param publishingOutboundPortURI   The URI of the outbound port requiring {@link PublishingCI}.
	 * @param receivingInboundPortURI     The URI of the inbound port offering {@link ReceivingCI}.
	 * @throws Exception                  If an error occurs during initialization or port publication.
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
		
		// Connect the registration port
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
		
		// Disconnect the registration port
		this.doPortDisconnection(registrationOutboundPort.getPortURI());
		
		// Disconnect the publishing port if connected
		if (brokerPublishingPortURI != null) this.doPortDisconnection(publishingOutboundPort.getPortURI());
		
		super.finalise();
	}
	
	@Override
	public synchronized void shutdown() throws ComponentShutdownException {
		
		try {
			// Unpublish ports
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
	 * Receives a {@code message} from {@code channel}.
	 * 
	 * <p>
	 * Should be {@code Override} for custom processing in the concrete client component.
	 * </p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code message != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param channel		name of the channel from which {@code messsage} is received.
	 * @param message		message received by the component.
	 */
	public Void receiveService(String channel, MessageI message) {
		// Precondition checks
		assert(channel != null);
		assert(!channel.isEmpty());
		assert(message != null);
		
		this.logMessage("Client received message '" + message + "' from the channel : " + channel);
		return null;
	}

	/**
	 * Receives {@code messages} from {@code channel}.
	 * 
	 * <p>
	 * Should be {@code Override} for custom processing in the concrete client component.
	 * </p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code messages != null && messages.size() > 0}
	 * pre	{@code Stream.of(messages).allMatch(m -> m != null)}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param channel		name of the channel from which {@code messsage} is received.
	 * @param messages		array of messages received by the component.
	 */
	public Void receiveService(String channel, MessageI[] messages) {
		// Precondition checks
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