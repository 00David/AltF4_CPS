package fr.sorbonne_u.clients.plugins;

import fr.sorbonne_u.broker.component.Broker;
import fr.sorbonne_u.clients.component.Client;
import fr.sorbonne_u.clients.connectors.RegistrationServiceConnector;
import fr.sorbonne_u.clients.ports.RegistrationOutboundPort;
import fr.sorbonne_u.components.AbstractPlugin;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.cps.pubsub.exceptions.AlreadyRegisteredException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownClientException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.cps.pubsub.plugins.interfaces.ClientRegistrationI;
import fr.sorbonne_u.cps.pubsub.plugins.interfaces.ClientSubscriptionI;

/**
 * The {@link Client} plug-in having a {@link RegistrationOutboundPort}.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true} // no more invariant
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class ClientRegistrationPlugin extends AbstractPlugin implements ClientRegistrationI {

	private static final long serialVersionUID = 612748100445996427L;
	
	// REGISTRATION
	
	/** Registration outbound port, set up after plug-in installation + initialization */
	protected RegistrationOutboundPort registrationOutboundPort;
	
	/** {@link Broker} publication inbound port URI, got after registration on the Broker */
	protected String brokerPublishingPortURI;
	
	/**
	 * Creates a {@link Client} registration plug-in.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true} // no precondition
	 * post {@code true} // no postcondition
	 * </pre>
	 */
	public ClientRegistrationPlugin(){}
	
	// -------------------------------------------------------------------------
	// Life cycle
	// -------------------------------------------------------------------------
	
	@Override
	public void	installOn(ComponentI owner) throws Exception {
		super.installOn(owner);
		this.addRequiredInterface(RegistrationCI.class);
	}
	
	@Override
	public void	initialise() throws Exception {
		super.initialise();
		this.registrationOutboundPort = new RegistrationOutboundPort(this.getOwner());
		this.registrationOutboundPort.publishPort();
		
		this.getOwner().doPortConnection(this.registrationOutboundPort.getPortURI(),
				Broker.registrationPortURI(),
				RegistrationServiceConnector.class.getCanonicalName());
	}
	
	@Override
	public void	finalise() throws Exception {
		this.getOwner().doPortDisconnection(this.registrationOutboundPort.getPortURI());
		super.finalise();
	}
	
	@Override
	public void uninstall() throws Exception {
		this.registrationOutboundPort.unpublishPort();
		this.removeRequiredInterface(RegistrationCI.class);
		super.uninstall();
	}
	
	// -------------------------------------------------------------------------
	// Plug-in services implementation
	// -------------------------------------------------------------------------
	
	// Precondition and postcondition verifications are delegated to the port used

	@Override
	public boolean registered() {
		Client owner = (Client)this.getOwner();
		try {
			String[] receivingInboundPortsUri = owner.findInboundPortURIsFromInterface(ReceivingCI.class);
			assert(receivingInboundPortsUri.length == 1); //We expect there is only 1 receiving port
			return registrationOutboundPort.registered(receivingInboundPortsUri[0]);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public boolean registered(RegistrationClass rc) throws UnknownClientException {
		Client owner = (Client)this.getOwner();
		try {
			String[] receivingInboundPortsUri = owner.findInboundPortURIsFromInterface(ReceivingCI.class);
			assert(receivingInboundPortsUri.length == 1); //We expect there is only 1 receiving port
			return registrationOutboundPort.registered(receivingInboundPortsUri[0], rc);
		} catch (UnknownClientException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public void register(RegistrationClass rc) throws AlreadyRegisteredException {
		Client owner = (Client)this.getOwner();
		try {
			String[] receivingInboundPortsUri = owner.findInboundPortURIsFromInterface(ReceivingCI.class);
			assert(receivingInboundPortsUri.length == 1); //We expect there is only 1 receiving port
			brokerPublishingPortURI = registrationOutboundPort.register(receivingInboundPortsUri[0], rc);
		} catch (AlreadyRegisteredException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void modifyServiceClass(RegistrationClass rc) throws UnknownClientException, AlreadyRegisteredException {
		Client owner = (Client)this.getOwner();
		try {
			String[] receivingInboundPortsUri = owner.findInboundPortURIsFromInterface(ReceivingCI.class);
			assert(receivingInboundPortsUri.length == 1); //We expect there is only 1 receiving port
			registrationOutboundPort.modifyServiceClass(receivingInboundPortsUri[0], rc);
		} catch (UnknownClientException | AlreadyRegisteredException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void unregister() throws UnknownClientException {
		Client owner = (Client)this.getOwner();
		try {
			String[] receivingInboundPortsUri = owner.findInboundPortURIsFromInterface(ReceivingCI.class);
			assert(receivingInboundPortsUri.length == 1); //We expect there is only 1 receiving port
			registrationOutboundPort.unregister(receivingInboundPortsUri[0]);
		} catch (UnknownClientException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	// -------------------------------------------------------------------------
	// Plug-in services used as a facade for other plug-ins
	// -------------------------------------------------------------------------
	
	// Precondition and postcondition verifications are delegated to the port used
	// ALL EXCEPTIONS ARE PROPAGATED
	
	/**
	 * @see ClientSubscriptionI#channelExist(String)
	 */
	@SuppressWarnings("javadoc")
	public boolean channelExist(String channel) throws Exception {
		return registrationOutboundPort.channelExist(channel);
	}
	
	/**
	 * @see ClientSubscriptionI#channelAuthorised(String)
	 */
	@SuppressWarnings("javadoc")
	public boolean channelAuthorised(String channel) throws	Exception {
		Client owner = (Client)this.getOwner();
		String[] receivingInboundPortsUri = owner.findInboundPortURIsFromInterface(ReceivingCI.class);
		assert(receivingInboundPortsUri.length == 1); //We expect there is only 1 receiving port
		return registrationOutboundPort.channelAuthorised(receivingInboundPortsUri[0], channel);
	}
	
	/**
	 * @see ClientSubscriptionI#subscribed(String)
	 */
	@SuppressWarnings("javadoc")
	public boolean subscribed(String channel) throws Exception {
		Client owner = (Client)this.getOwner();
		String[] receivingInboundPortsUri = owner.findInboundPortURIsFromInterface(ReceivingCI.class);
		assert(receivingInboundPortsUri.length == 1); //We expect there is only 1 receiving port
		return registrationOutboundPort.subscribed(receivingInboundPortsUri[0], channel);
	}
	
	/**
	 * @see ClientSubscriptionI#subscribe(String, MessageFilterI)
	 */
	@SuppressWarnings("javadoc")
	public void subscribe(String channel, MessageFilterI filter) throws Exception {
		Client owner = (Client)this.getOwner();
		String[] receivingInboundPortsUri = owner.findInboundPortURIsFromInterface(ReceivingCI.class);
		assert(receivingInboundPortsUri.length == 1); //We expect there is only 1 receiving port
		registrationOutboundPort.subscribe(receivingInboundPortsUri[0], channel, filter);
	}
	
	/**
	 * @see ClientSubscriptionI#unsubscribe(String)
	 */
	@SuppressWarnings("javadoc")
	public void	unsubscribe(String channel) throws Exception {
		Client owner = (Client)this.getOwner();
		String[] receivingInboundPortsUri = owner.findInboundPortURIsFromInterface(ReceivingCI.class);
		assert(receivingInboundPortsUri.length == 1); //We expect there is only 1 receiving port
		registrationOutboundPort.unsubscribe(receivingInboundPortsUri[0], channel);
	}
	
	/**
	 * @see ClientSubscriptionI#modifyFilter(String, MessageFilterI)
	 */
	@SuppressWarnings("javadoc")
	public void modifyFilter(String channel, MessageFilterI filter) throws Exception {
		Client owner = (Client)this.getOwner();
		String[] receivingInboundPortsUri = owner.findInboundPortURIsFromInterface(ReceivingCI.class);
		assert(receivingInboundPortsUri.length == 1); //We expect there is only 1 receiving port
		registrationOutboundPort.modifyFilter(receivingInboundPortsUri[0], channel, filter);
	}
	
	// -------------------------------------------------------------------------
	// Getter
	// -------------------------------------------------------------------------
	
	/**
	 * Returns the broker URI offering either {@link PublishingCI} or {@link PrivilegedClientCI} interface.
	 * 
	 * <p>It can return a null String if the owning client component is not registered on the broker</p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @return the broker URI offering either {@link PublishingCI} or {@link PrivilegedClientCI} interface, or {@code null}.
	 */
	public String getBrokerPublishingPortURI() {
		return brokerPublishingPortURI;
	}
	
}
