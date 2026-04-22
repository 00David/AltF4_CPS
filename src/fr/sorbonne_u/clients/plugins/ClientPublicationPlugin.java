package fr.sorbonne_u.clients.plugins;

import java.util.ArrayList;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.clients.connectors.ClientPublishingServiceConnector;
import fr.sorbonne_u.clients.ports.ClientPublishingOutboundPort;
import fr.sorbonne_u.clients.ports.ClientRegistrationOutboundPort;
import fr.sorbonne_u.components.AbstractPlugin;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.tasks_management.connection.AbnormalTerminationNotificationInboundPort;
import fr.sorbonne_u.components.tasks_management.interfaces.AbnormalTerminationNotificationCI;
import fr.sorbonne_u.cps.pubsub.exceptions.UnauthorisedClientException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownClientException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.plugins.interfaces.ClientPublicationI;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * The {@link Client} plugin having a {@link ClientPublishingOutboundPort} and a {@link AbnormalTerminationNotificationInboundPort}.
 * 
 * <p>
 * This plugin needs to be created after registering on the {@link Broker}, in order to pass 
 * a valid {@code brokerPublishingPortURI} argument to the constructor.
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
 * invariant	{@code registrationOutboundPort != null}
 * invariant	{@code publishingOutboundPortURI != null && !publishingOutboundPortURI.isEmpty()}
 * invariant	{@code brokerPublishingPortURI != null && !brokerPublishingPortURI.isEmpty()}
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class ClientPublicationPlugin extends AbstractPlugin implements ClientPublicationI {
	
	private static final long serialVersionUID = 4889836074410437984L;
	
	// NOTIFICATION
	
	/** Notification inbound port, set up after plugin installation + initialization */
	protected AbnormalTerminationNotificationInboundPort notificationInboundPort;
	
	// RECEPTION
	
	/** {@link Client} reception inbound port URI */
	protected final String receivingInboundPortURI;
	
	// REGISTRATION
	
	/** Registration outbound port, owned by {@link ClientRegistrationPlugin} */
	protected final ClientRegistrationOutboundPort registrationOutboundPort;
	
	// PUBLICATION
	
	/** {@link Client} publication outbound port URI */
	protected final String publishingOutboundPortURI;
	
	/** Publication outbound port, set up after plugin installation + initialization */
	protected ClientPublishingOutboundPort publishingOutboundPort;
	
	/** {@link Broker} publication inbound port URI, got via {@link ClientRegistrationPlugin} after registration */
	protected final String brokerPublishingPortURI;
	
	/**
	 * Creates a {@link Client} publishing plugin.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
	 * pre	{@code registrationOutboundPort != null}
	 * pre	{@code publishingOutboundPortURI != null && !publishingOutboundPortURI.isEmpty()}
	 * pre	{@code brokerPublishingPortURI != null && !brokerPublishingPortURI.isEmpty()}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param receivingInboundPortURI		the client inbound port URI offering {@link ReceivingCI} interface.
	 * @param registrationOutboundPort		the client outbound port requiring {@link RegistrationCI} interface.
	 * @param publishingOutboundPortURI		the client outbound port URI requiring {@link PublishingCI} interface, used for creating the {@link ClientPublishingOutboundPort}.
	 * @param brokerPublishingPortURI		the {@link Broker} inbound port URI offering {@link PublishingCI} or {@link PrivilegedClientCI} interface.
	 */
	public ClientPublicationPlugin(String receivingInboundPortURI, ClientRegistrationOutboundPort registrationOutboundPort,
			String publishingOutboundPortURI, String brokerPublishingPortURI){
		// Precondition checks
		assert(receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()) : 
			new PreconditionException("receivingInboundPortURI cannot be null or empty");
		assert(registrationOutboundPort != null) : 
			new PreconditionException("registrationOutboundPort cannot be null");
		assert(publishingOutboundPortURI != null && !publishingOutboundPortURI.isEmpty()) : 
			new PreconditionException("publishingOutboundPortURI cannot be null or empty");
		assert(brokerPublishingPortURI != null && !brokerPublishingPortURI.isEmpty()) : 
			new PreconditionException("brokerPublishingPortURI cannot be null or empty");
		
		this.receivingInboundPortURI = receivingInboundPortURI;
		this.registrationOutboundPort = registrationOutboundPort;
		this.publishingOutboundPortURI = publishingOutboundPortURI;
		this.brokerPublishingPortURI = brokerPublishingPortURI;
	}
	
	// -------------------------------------------------------------------------
	// Life cycle
	// -------------------------------------------------------------------------
	
	@Override
	public void	installOn(ComponentI owner) throws Exception {
		super.installOn(owner);
		this.addRequiredInterface(PublishingCI.class);
		this.addOfferedInterface(AbnormalTerminationNotificationCI.class);
	}
	
	@Override
	public void	initialise() throws Exception {
		super.initialise();
		this.publishingOutboundPort = new ClientPublishingOutboundPort(publishingOutboundPortURI, this.getOwner());
		this.publishingOutboundPort.publishPort();
		
		this.getOwner().doPortConnection(this.publishingOutboundPort.getPortURI(),
				brokerPublishingPortURI,
				ClientPublishingServiceConnector.class.getCanonicalName());
		
		this.notificationInboundPort = new AbnormalTerminationNotificationInboundPort(this.getOwner());
		this.notificationInboundPort.publishPort();
	}
	
	@Override
	public void	finalise() throws Exception {
		this.getOwner().doPortDisconnection(this.publishingOutboundPort.getPortURI());
		super.finalise();
	}
	
	@Override
	public void uninstall() throws Exception {
		this.publishingOutboundPort.unpublishPort();
		this.removeRequiredInterface(PublishingCI.class);
		this.notificationInboundPort.unpublishPort();
		this.removeOfferedInterface(AbnormalTerminationNotificationCI.class);
		super.uninstall();
	}
	
	// -------------------------------------------------------------------------
	// Plug-in services implementation
	// -------------------------------------------------------------------------

	// Precondition and postcondition verifications are delegated to the port used
	
	@Override
	public boolean channelExist(String channel) {
		try {
			return registrationOutboundPort.channelExist(channel);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public boolean channelAuthorised(String channel) throws UnknownClientException, UnknownChannelException {
		try {
			return registrationOutboundPort.channelAuthorised(receivingInboundPortURI, channel);
		} catch (UnknownClientException | UnknownChannelException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public void publish(String channel, MessageI message)
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException {
		try {
			publishingOutboundPort.publish(receivingInboundPortURI, channel, message);
		} catch (UnknownClientException | UnknownChannelException | UnauthorisedClientException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void publish(String channel, ArrayList<MessageI> messages)
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException {
		try {
			publishingOutboundPort.publish(receivingInboundPortURI, channel, messages);
		} catch (UnknownClientException | UnknownChannelException | UnauthorisedClientException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void asyncPublishAndNotify(String channel, MessageI message) {
		try {
			publishingOutboundPort.asyncPublishAndNotify(receivingInboundPortURI, channel, message, notificationInboundPort.getPortURI());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void asyncPublishAndNotify(String channel, ArrayList<MessageI> messages) {
		try {
			publishingOutboundPort.asyncPublishAndNotify(receivingInboundPortURI, channel, messages, notificationInboundPort.getPortURI());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
