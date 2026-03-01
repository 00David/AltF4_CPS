package fr.sorbonne_u.clients.plugins;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.clients.connectors.ClientRegistrationServiceConnector;
import fr.sorbonne_u.clients.ports.ClientRegistrationOutboundPort;
import fr.sorbonne_u.components.AbstractPlugin;
import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.cps.pubsub.exceptions.AlreadyRegisteredException;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownClientException;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.cps.pubsub.plugins.interfaces.ClientRegistrationI;

/**
 * The {@link Client} plugin having a {@link ClientRegistrationOutboundPort}.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
 * invariant	{@code registrationOutboundPortURI != null && !registrationOutboundPortURI.isEmpty()}
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class ClientRegistrationPlugin extends AbstractPlugin implements ClientRegistrationI {

	private static final long serialVersionUID = 612748100445996427L;
	
	// Reception
	protected final String receivingInboundPortURI;
	
	// Registration
	protected final String registrationOutboundPortURI;
	protected ClientRegistrationOutboundPort registrationOutboundPort; // Set up after plugin installation + initialization
	
	// Broker inbound port
	protected String brokerPublishingPortURI; // Set up after registration on the Broker
	
	/**
	 * Creates a {@link Client} registration plugin.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
	 * pre	{@code registrationOutboundPortURI != null && !registrationOutboundPortURI.isEmpty()}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param receivingInboundPortURI		the client inbound port URI offering {@link ReceivingCI} interface.
	 * @param registrationOutboundPortURI	the client outbound port URI requiring {@link RegistrationCI} interface, used for creating the {@link ClientRegistrationOutboundPort}.
	 */
	public ClientRegistrationPlugin(String receivingInboundPortURI, String registrationOutboundPortURI){
		// Precondition checks
		assert(receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty());
		assert(registrationOutboundPortURI != null && !registrationOutboundPortURI.isEmpty());
		
		this.receivingInboundPortURI = receivingInboundPortURI;
		this.registrationOutboundPortURI = registrationOutboundPortURI;
	}
	
	// -------------------------------------------------------------------------
	// Life cycle
	// -------------------------------------------------------------------------
	
	@Override
	public void	installOn(ComponentI owner) throws Exception {
		super.installOn(owner);
		this.addRequiredInterface(RegistrationCI.class);
		this.registrationOutboundPort = new ClientRegistrationOutboundPort(registrationOutboundPortURI, this.getOwner());
		this.registrationOutboundPort.publishPort();
	}
	
	@Override
	public void	initialise() throws Exception {
		this.getOwner().doPortConnection(this.registrationOutboundPort.getPortURI(),
				Broker.registrationPortURI(),
				ClientRegistrationServiceConnector.class.getCanonicalName());
		super.initialise();
	}
	
	@Override
	public void	finalise() throws Exception {
		this.getOwner().doPortDisconnection(this.registrationOutboundPort.getPortURI());
	}
	
	@Override
	public void uninstall() throws Exception {
		this.registrationOutboundPort.unpublishPort();
		this.removeRequiredInterface(RegistrationCI.class);
	}
	
	// -------------------------------------------------------------------------
	// Plug-in services implementation
	// -------------------------------------------------------------------------
	
	// Precondition and postcondition verifications are delegated to the port used

	@Override
	public boolean registered() {
		try {
			return registrationOutboundPort.registered(receivingInboundPortURI);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public boolean registered(RegistrationClass rc) throws UnknownClientException {
		try {
			return registrationOutboundPort.registered(receivingInboundPortURI, rc);
		} catch (UnknownClientException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public void register(RegistrationClass rc) throws AlreadyRegisteredException {
		try {
			registrationOutboundPort.register(receivingInboundPortURI, rc);
		} catch (AlreadyRegisteredException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void modifyServiceClass(RegistrationClass rc) throws UnknownClientException, AlreadyRegisteredException {
		try {
			registrationOutboundPort.modifyServiceClass(receivingInboundPortURI, rc);
		} catch (UnknownClientException | AlreadyRegisteredException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void unregister() throws UnknownClientException {
		try {
			registrationOutboundPort.unregister(receivingInboundPortURI);
		} catch (UnknownClientException e) {
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	// -------------------------------------------------------------------------
	// Getters
	// -------------------------------------------------------------------------
	
	/**
	 * Returns the client port requiring the {@link RegistrationCI} interface.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @return the client port requiring the {@link RegistrationCI} interface.
	 */
	public ClientRegistrationOutboundPort getRegistrationOutboundPort() {
		return registrationOutboundPort;
	}
	
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
