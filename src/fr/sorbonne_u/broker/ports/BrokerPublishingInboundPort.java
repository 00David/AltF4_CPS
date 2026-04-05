package fr.sorbonne_u.broker.ports;

import java.io.Serializable;
import java.util.ArrayList;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.exceptions.BCMRuntimeException;
import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.components.tasks_management.BCM4JavaComponentThread;
import fr.sorbonne_u.components.tasks_management.connection.AbnormalTerminationNotificationOutboundPort;
import fr.sorbonne_u.cps.pubsub.exceptions.UnauthorisedClientException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownClientException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * Inbound port of a {@link Broker}, allowing the handling of client registration.<br>
 * Delegates the processing to its owner component, a {@link Broker}.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code notificationPort != null}
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class BrokerPublishingInboundPort
extends		AbstractInboundPort
implements	PublishingCI {

	private static final long serialVersionUID = 1878031196993861332L;
	
	protected final AbnormalTerminationNotificationOutboundPort	notificationPort;

	/**
	 * Creates the inbound port with the given {@code uri}, whose owner component {@code owner} is a {@link Broker}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre  {@code uri != null && !uri.isEmpty()}
	 * pre  {@code owner != null}
	 * pre  {@code owner instanceof Broker}
	 * pre  {@code notificationPort != null}
	 * post class invariant satisfied
	 * </pre>
	 * 
	 * @param uri    					the URI of the port.
	 * @param owner  					the component owning the port.
	 * @param notificationPort  		outport port requiring the component interface {@code AbnormalTerminationNotificationCI}.
	 * @throws Exception 				if an error occurs while creating the port
	 */
	public BrokerPublishingInboundPort(String uri, ComponentI owner, 
			AbnormalTerminationNotificationOutboundPort notificationPort) throws Exception {
		super(uri, PublishingCI.class, owner) ;
		// Precondition checks
		assert(uri != null && !uri.isEmpty()) : new PreconditionException("port uri cannot be null or empty");
		assert(owner != null) : new PreconditionException("owner cannot be null");
		assert(owner instanceof Broker) : new PreconditionException("owner must be a Broker");
		assert(notificationPort != null) : new PreconditionException("notificationPort cannot be null");
		
		this.notificationPort = notificationPort;
	}
	
	/**
	 * Creates the inbound port with the given {@code uri}, whose owner component {@code owner} is a {@link Broker}.<br>
	 * Used by {@link BrokerPrivilegedInboundPort} constructor.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre  {@code uri != null && !uri.isEmpty()}
	 * pre  {@code owner != null}
	 * pre  {@code owner instanceof Broker}
	 * pre  {@code notificationPort != null}
	 * post class invariant satisfied
	 * </pre>
	 * 
	 * @param uri    					the URI of the port.
	 * @param implementedInterface    	the implemented interface, extending {@link OfferedCI}.
	 * @param owner  					the component owning the port.
	 * @param notificationPort  		outport port requiring the component interface {@code AbnormalTerminationNotificationCI}.
	 * @throws Exception 				if an error occurs while creating the port
	 */
	public BrokerPublishingInboundPort(String uri, Class<? extends OfferedCI> implementedInterface, ComponentI owner, 
			AbnormalTerminationNotificationOutboundPort notificationPort) throws Exception {
		super(uri, implementedInterface, owner);
		// Precondition checks
		assert(uri != null && !uri.isEmpty()) : new PreconditionException("port uri cannot be null or empty");
		assert(owner != null) : new PreconditionException("owner cannot be null");
		assert(owner instanceof Broker) : new PreconditionException("owner must be a Broker");
		assert(notificationPort != null) : new PreconditionException("notificationPort cannot be null");
		
		this.notificationPort = notificationPort;
	}

	// Precondition and postcondition verifications are delegated to the Broker

	@Override
	public void publish(String receptionPortURI, String channel, MessageI message) throws Exception {
		ArrayList<MessageI> messages =  new ArrayList<MessageI>();
		messages.add(message);
		this.getOwner().runTask(
				owner -> {
					try {
						((Broker)owner).publishService(receptionPortURI, channel, messages, null);
					} catch (Exception e) {
						e.printStackTrace();
					}
				});
	}

	@Override
	public void publish(String receptionPortURI, String channel, ArrayList<MessageI> messages) throws Exception {
		this.getOwner().runTask(
				owner -> {
					try {
						((Broker)owner).publishService(receptionPortURI, channel, messages, null);
					} catch (Exception e) {
						e.printStackTrace();
					}
				});
	}

	@Override
	public void asyncPublishAndNotify(String receptionPortURI, String channel, MessageI message, String notificationInbounhdPortURI) throws Exception {
		ArrayList<MessageI> messages =  new ArrayList<MessageI>();
		messages.add(message);
		this.getOwner().runTask(
				owner -> {
					try {
						((BCM4JavaComponentThread)Thread.currentThread()).
							setServiceInvocationInfo(
									notificationPort, // Broker notification outbound port
									notificationInbounhdPortURI, // Client notification inbound port URI
									PublishingCI.class,
									this.getPortURI(),
									"asyncPublishAndNotify",
									new Serializable[]{receptionPortURI, channel, messages});
						((Broker)owner).publishService(receptionPortURI, channel, messages, null);
					} catch (UnknownClientException | UnknownChannelException | UnauthorisedClientException e) {
						throw new BCMRuntimeException(e);
					} catch (Exception e) {
						e.printStackTrace();
					}
				});
	}

	@Override
	public void asyncPublishAndNotify(String receptionPortURI, String channel, ArrayList<MessageI> messages, String notificationInbounhdPortURI) throws Exception {
		this.getOwner().runTask(
				owner -> {
					try {
						((BCM4JavaComponentThread)Thread.currentThread()).
							setServiceInvocationInfo(
									notificationPort, // Broker notification outbound port
									notificationInbounhdPortURI, // Client notification inbound port URI
									PublishingCI.class,
									this.getPortURI(),
									"asyncPublishAndNotify",
									new Serializable[]{receptionPortURI, channel, messages});
						((Broker)owner).publishService(receptionPortURI, channel, messages, null);
					} catch (UnknownClientException | UnknownChannelException | UnauthorisedClientException e) {
						throw new BCMRuntimeException(e);
					} catch (Exception e) {
						e.printStackTrace();
					}
				});
	}
	
}
