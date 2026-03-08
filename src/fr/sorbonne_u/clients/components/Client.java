package fr.sorbonne_u.clients.components;

import fr.sorbonne_u.clients.plugins.ClientRegistrationPlugin;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.exceptions.PostconditionException;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * The abstract class providing a component that acts as a client of the publish/subscribe system.
 * 
 * <p>The client is created only with a registration plugin. Concrete sub-classes can add/use more plugins.</p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty}
 * invariant	{@code registrationPlugin != null && registrationPlugin instanceof ClientRegistrationPlugin}
 * invariant 	{@code registrationPlugin.isInitialised()}
 * invariant	{@code registrationClass != null}
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public abstract class Client extends AbstractComponent {
	
	// RECEPTION
	
	/** Reception inbound port URI (but used as the component identifier within the Broker) */
	protected final String receivingInboundPortURI;
	
	// REGISTRATION
	
	/** Client registration plugin. Every concrete client has it initialized. */
	protected final ClientRegistrationPlugin registrationPlugin;
	
	/** Client registration class */
	protected RegistrationClass registrationClass;
	
	/**
	 * Creates a client component of the publish/subscribe system.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receivingPortURI != null && !receivingPortURI.isEmpty()}
	 * pre	{@code publishingOutboundPortURI != null && !publishingOutboundPortURI.isEmpty()}
	 * pre	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
	 * pre	{@code registrationClass != null}
	 * pre	{@code nbThreads >= 0 && nbSchedulableThreads >= 0}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param receivingInboundPortURI     	The URI of the inbound port offering {@link ReceivingCI}.
	 * @param registrationPluginURI			The URI of the registration plugin.
	 * @param registrationOutboundPortURI 	The URI of the outbound port requiring {@link RegistrationCI}.
	 * @param registrationClass				The registration class of the component.
	 * @param nbThreads						Number of threads to be created in the component pool.
	 * @param nbSchedulableThreads			Number of threads to be created in the component schedulable pool.
	 * @throws Exception                  	If an error occurs during registration plugin installation.
	 */
	protected Client(String receivingInboundPortURI, String registrationPluginURI, String registrationOutboundPortURI, 
			RegistrationClass registrationClass, int nbThreads, int nbSchedulableThreads) throws Exception {
		super(nbThreads,nbSchedulableThreads);
		
		// Precondition checks
		assert(receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()) : new PreconditionException("receivingInboundPortURI cannot be empty or null");
		assert(registrationPluginURI != null && !registrationPluginURI.isEmpty()) : new PreconditionException("registrationPluginURI cannot be empty or null");
		assert(registrationOutboundPortURI != null && !registrationOutboundPortURI.isEmpty()) : new PreconditionException("registrationOutboundPortURI cannot be empty or null");
		assert(registrationClass != null) : new PreconditionException("registrationClass cannot be null");
		assert(nbThreads >= 0 && nbSchedulableThreads >= 0) : new PreconditionException("cannot have a negative number of threads");
		
		this.receivingInboundPortURI = receivingInboundPortURI;
		this.registrationClass = registrationClass;
		
		// Install the registration plug-in.
		this.registrationPlugin = new ClientRegistrationPlugin(receivingInboundPortURI, registrationOutboundPortURI);
		this.registrationPlugin.setPluginURI(registrationPluginURI);
		this.installPlugin(this.registrationPlugin);
		
		// Invariant checks
		AbstractComponent.checkImplementationInvariant(this);
		AbstractComponent.checkInvariant(this);
		
		// Postcondition checks
		assert registrationPlugin.isInitialised() : 
			new PostconditionException("Registration plugin must be initialized");
		assert	this.isPortExisting(registrationOutboundPortURI) : 
			new PostconditionException("The component must have a port with URI " + registrationOutboundPortURI);
		assert	this.findPortFromURI(registrationOutboundPortURI).getImplementedInterface().equals(RegistrationCI.class) : 
			new PostconditionException("The component must have a port with implemented interface RegistrationCI");
		assert	this.findPortFromURI(registrationOutboundPortURI).isPublished() :
			new PostconditionException("The component must have a port published with URI " + registrationOutboundPortURI);
	}
	
	/**
	 * Creates a client component of the publish/subscribe system, by providing the {@code reflectionInboundPortURI} of the component.
	 * 
	 * <p>Used by test sub-classes.</p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receivingPortURI != null && !receivingPortURI.isEmpty()}
	 * pre	{@code publishingOutboundPortURI != null && !publishingOutboundPortURI.isEmpty()}
	 * pre	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
	 * pre	{@code registrationClass != null}
	 * pre	{@code nbThreads >= 0 && nbSchedulableThreads >= 0}
	 * pre	{@code reflectionInboundPortURI != null && !reflectionInboundPortURI.isEmpty()}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param receivingInboundPortURI     	The URI of the inbound port offering {@link ReceivingCI}.
	 * @param registrationPluginURI			The URI of the registration plugin.
	 * @param registrationOutboundPortURI 	The URI of the outbound port requiring {@link RegistrationCI}.
	 * @param registrationClass				The registration class of the component.
	 * @param nbThreads						Number of threads to be created in the component pool.
	 * @param nbSchedulableThreads			Number of threads to be created in the component schedulable pool.
	 * @param reflectionInboundPortURI		URI of the reflection inbound port to be created in this component.
	 * @throws Exception                  	If an error occurs during registration plugin installation.
	 */
	protected Client(String receivingInboundPortURI, String registrationPluginURI, String registrationOutboundPortURI, 
			RegistrationClass registrationClass, int nbThreads, int nbSchedulableThreads, String reflectionInboundPortURI) throws Exception {
		super(reflectionInboundPortURI, nbThreads,nbSchedulableThreads);
		
		// Precondition checks
		assert(receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()) : new PreconditionException("receivingInboundPortURI cannot be empty or null");
		assert(registrationPluginURI != null && !registrationPluginURI.isEmpty()) : new PreconditionException("registrationPluginURI cannot be empty or null");
		assert(registrationOutboundPortURI != null && !registrationOutboundPortURI.isEmpty()) : new PreconditionException("registrationOutboundPortURI cannot be empty or null");
		assert(registrationClass != null) : new PreconditionException("registrationClass cannot be empty or null");
		assert(nbThreads >= 0 && nbSchedulableThreads >= 0) : new PreconditionException("cannot have a negative number of threads");
		assert(reflectionInboundPortURI != null && !reflectionInboundPortURI.isEmpty()) : new PreconditionException("reflectionInboundPortURI cannot be empty or null");
		
		this.receivingInboundPortURI = receivingInboundPortURI;
		this.registrationClass = registrationClass;
		
		// Install the registration plug-in.
		this.registrationPlugin = new ClientRegistrationPlugin(receivingInboundPortURI, registrationOutboundPortURI);
		this.registrationPlugin.setPluginURI(registrationPluginURI);
		this.installPlugin(this.registrationPlugin);
		
		// Invariant checks
		AbstractComponent.checkImplementationInvariant(this);
		AbstractComponent.checkInvariant(this);
		
		// Postcondition checks
		assert registrationPlugin.isInitialised() : 
			new PostconditionException("Registration plugin must be initialized");
		assert	this.isPortExisting(registrationOutboundPortURI) : 
			new PostconditionException("The component must have a port with URI " + registrationOutboundPortURI);
		assert	this.findPortFromURI(registrationOutboundPortURI).getImplementedInterface().equals(RegistrationCI.class) : 
			new PostconditionException("The component must have a port with implemented interface RegistrationCI");
		assert	this.findPortFromURI(registrationOutboundPortURI).isPublished() :
			new PostconditionException("The component must have a port published with URI " + registrationOutboundPortURI);
	}
}
