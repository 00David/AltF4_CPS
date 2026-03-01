package fr.sorbonne_u.clients.components;

import fr.sorbonne_u.clients.plugins.ClientRegistrationPlugin;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;

/**
 * The abstract class providing a component that acts as a client of the publish/subscribe system.
 * 
 * <p>The client is created only with a registration plugin. Concrete sub-classes can add/use more plugins.</p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty}
 * invariant	{@code registrationPlugin != null && registrationPlugin instanceof ClientRegistrationPlugin}
 * invariant	{@code registrationClass != null}
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public abstract class Client extends AbstractComponent {
	
	// Reception (but used as the component identifier within the Broker)
	protected final String receivingInboundPortURI;
	
	// Registration
	protected final ClientRegistrationPlugin registrationPlugin;
	protected RegistrationClass registrationClass;
	
	/**
	 * Creates a client component of the publish/subscribe system, by setting the registration plugin name as "registration".
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receivingPortURI != null && !receivingPortURI.isEmpty()}
	 * pre	{@code publishingOutboundPortURI != null && !publishingOutboundPortURI.isEmpty()}
	 * pre	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param receivingInboundPortURI     	The URI of the inbound port offering {@link ReceivingCI}.
	 * @param registrationOutboundPortURI 	The URI of the outbound port requiring {@link RegistrationCI}.
	 * @param registrationClass				The registration class of the component.
	 * @throws Exception                  	If an error occurs during registration plugin installation.
	 */
	protected Client(String receivingInboundPortURI, String registrationOutboundPortURI, RegistrationClass registrationClass) throws Exception {
		this(receivingInboundPortURI, "registration", registrationOutboundPortURI, registrationClass);
	}
	
	/**
	 * Creates a client component of the publish/subscribe system.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receivingPortURI != null && !receivingPortURI.isEmpty()}
	 * pre	{@code publishingOutboundPortURI != null && !publishingOutboundPortURI.isEmpty()}
	 * pre	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param receivingInboundPortURI     	The URI of the inbound port offering {@link ReceivingCI}.
	 * @param registrationPluginURI			The URI of the registration plugin.
	 * @param registrationOutboundPortURI 	The URI of the outbound port requiring {@link RegistrationCI}.
	 * @param registrationClass				The registration class of the component.
	 * @throws Exception                  	If an error occurs during registration plugin installation.
	 */
	protected Client(String receivingInboundPortURI, String registrationPluginURI, String registrationOutboundPortURI, RegistrationClass registrationClass) throws Exception {
		super(1,0);
		// Precondition checks
		assert(receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty());
		assert(registrationPluginURI != null && !registrationPluginURI.isEmpty());
		assert(registrationOutboundPortURI != null && !registrationOutboundPortURI.isEmpty());
		
		this.receivingInboundPortURI = receivingInboundPortURI;
		this.registrationClass = registrationClass;
		
		// Install the registration plug-in.
		this.registrationPlugin = new ClientRegistrationPlugin(receivingInboundPortURI, registrationOutboundPortURI);
		this.registrationPlugin.setPluginURI(registrationPluginURI);
		this.installPlugin(this.registrationPlugin);
		
		AbstractComponent.checkImplementationInvariant(this);
		AbstractComponent.checkInvariant(this);
	}
}
