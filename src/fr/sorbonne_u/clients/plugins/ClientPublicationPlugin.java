package fr.sorbonne_u.clients.plugins;

import java.util.ArrayList;

import fr.sorbonne_u.broker.component.Broker;
import fr.sorbonne_u.clients.component.Client;
import fr.sorbonne_u.clients.connectors.PublishingServiceConnector;
import fr.sorbonne_u.clients.ports.PublishingOutboundPort;
import fr.sorbonne_u.components.AbstractPlugin;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.tasks_management.connection.AbnormalTerminationNotificationInboundPort;
import fr.sorbonne_u.components.tasks_management.interfaces.AbnormalTerminationNotificationCI;
import fr.sorbonne_u.cps.pubsub.exceptions.UnauthorisedClientException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownChannelException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownClientException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.plugins.interfaces.ClientPublicationI;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * The {@link Client} plug-in having a {@link PublishingOutboundPort} and a {@link AbnormalTerminationNotificationInboundPort}.
 * 
 * <p>
 * This plug-in needs to be created after registering on the {@link Broker}, in order to achieve 
 * a valid connection to an inbound {@link Broker} publishing port.
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code registrationPlugin != null && registrationPlugin.isInitialised()}
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class ClientPublicationPlugin extends AbstractPlugin implements ClientPublicationI {
	
	private static final long serialVersionUID = 4889836074410437984L;
	
	/** 
	 * <p>Registration plug-in, owning an outbound port requiring {@link RegistrationCI} interface. 
	 * Some local services are delegated to it.</p>

	 * <p>Its {@code getBrokerPublishingPortURI()} method is used for getting 
	 * the {@link Broker} inbound port URI offering {@link PublishingCI} interface.</p>
	 */
	protected final ClientRegistrationPlugin registrationPlugin;
	
	// NOTIFICATION
	
	/** Notification inbound port, set up after plugin installation + initialization */
	protected AbnormalTerminationNotificationInboundPort notificationInboundPort;
	
	// PUBLICATION
	
	/** Publication outbound port, set up after plugin installation + initialization */
	protected PublishingOutboundPort publishingOutboundPort;
	
	/**
	 * Creates a {@link Client} publishing plug-in.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code registrationPlugin != null && registrationPlugin.isInitialised()}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param registrationPlugin		The client registration plug-in owning an outbound port requiring {@link RegistrationCI} interface.
	 */
	public ClientPublicationPlugin(ClientRegistrationPlugin registrationPlugin){
		// Precondition checks
		assert(registrationPlugin != null  && registrationPlugin.isInitialised()) : 
			new PreconditionException("registrationPlugin must be initialised");
		
		this.registrationPlugin = registrationPlugin;
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
		
		this.publishingOutboundPort = new PublishingOutboundPort(this.getOwner());
		this.publishingOutboundPort.publishPort();
		
		// An error will be thrown here if the Client, through its registrationPlugin, has not yet been registered 
		// (registrationPlugin.getBrokerPublishingPortURI() == null)
		this.getOwner().doPortConnection(this.publishingOutboundPort.getPortURI(),
				registrationPlugin.getBrokerPublishingPortURI(),
				PublishingServiceConnector.class.getCanonicalName());
		
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

	// Precondition and postcondition verifications are delegated to the registration plug-in used
	
	@Override
	public boolean channelExist(String channel) {
		try {
			return registrationPlugin.channelExist(channel);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public boolean channelAuthorised(String channel) throws UnknownClientException, UnknownChannelException {
		try {
			return registrationPlugin.channelAuthorised(channel);
		} catch (UnknownClientException | UnknownChannelException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	
	// Precondition and postcondition verifications are delegated to the port used

	@Override
	public void publish(String channel, MessageI message)
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException {
		Client owner = (Client)this.getOwner();
		try {
			String[] receivingInboundPortsUri = owner.findInboundPortURIsFromInterface(ReceivingCI.class);
			assert(receivingInboundPortsUri.length == 1); //We expect there is only 1 receiving port
			publishingOutboundPort.publish(receivingInboundPortsUri[0], channel, message);
		} catch (UnknownClientException | UnknownChannelException | UnauthorisedClientException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void publish(String channel, ArrayList<MessageI> messages)
			throws UnknownClientException, UnknownChannelException, UnauthorisedClientException {
		Client owner = (Client)this.getOwner();
		try {
			String[] receivingInboundPortsUri = owner.findInboundPortURIsFromInterface(ReceivingCI.class);
			assert(receivingInboundPortsUri.length == 1); //We expect there is only 1 receiving port
			publishingOutboundPort.publish(receivingInboundPortsUri[0], channel, messages);
		} catch (UnknownClientException | UnknownChannelException | UnauthorisedClientException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void asyncPublishAndNotify(String channel, MessageI message) {
		Client owner = (Client)this.getOwner();
		try {
			String[] receivingInboundPortsUri = owner.findInboundPortURIsFromInterface(ReceivingCI.class);
			assert(receivingInboundPortsUri.length == 1); //We expect there is only 1 receiving port
			publishingOutboundPort.asyncPublishAndNotify(receivingInboundPortsUri[0], channel, message, notificationInboundPort.getPortURI());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void asyncPublishAndNotify(String channel, ArrayList<MessageI> messages) {
		Client owner = (Client)this.getOwner();
		try {
			String[] receivingInboundPortsUri = owner.findInboundPortURIsFromInterface(ReceivingCI.class);
			assert(receivingInboundPortsUri.length == 1); //We expect there is only 1 receiving port
			publishingOutboundPort.asyncPublishAndNotify(receivingInboundPortsUri[0], channel, messages, notificationInboundPort.getPortURI());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
