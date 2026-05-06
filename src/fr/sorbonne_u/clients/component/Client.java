package fr.sorbonne_u.clients.component;

import java.io.Serializable;

import fr.sorbonne_u.clients.plugins.ClientPrivilegedPlugin;
import fr.sorbonne_u.clients.plugins.ClientPublicationPlugin;
import fr.sorbonne_u.clients.plugins.ClientRegistrationPlugin;
import fr.sorbonne_u.clients.plugins.ClientSubscriptionPlugin;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.tasks_management.interfaces.AbnormalTerminationNotificationCI;
import fr.sorbonne_u.components.tasks_management.interfaces.AbnormalTerminationNotificationImplI;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.exceptions.InvariantException;
import fr.sorbonne_u.exceptions.PostconditionException;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * The abstract class providing a component that acts as a client of the publish/subscribe system.
 * 
 * <p>
 * A client can handle abnormal termination notifications thanks to its {@link AbnormalTerminationNotificationImplI} 
 * implementation and {@code notifyAbnormalTermination} method overriding.
 * </p>
 * 
 * <p>
 * The client is created only with a registration plug-in. 
 * Concrete sub-classes can add/use more plug-ins by calling local plug-in setup methods.
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty}
 * invariant	{@code registrationPlugin != null && registrationPlugin.isInitialised()}
 * invariant	the Client has exactly 1 registration outbound port implementing RegistrationCI. // informal
 * invariant	{@code registrationClass != null}
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public abstract class Client extends AbstractComponent implements AbnormalTerminationNotificationImplI {
	
	// REGISTRATION
	
	/** 
	 * Client registration plug-in. 
	 * Every concrete client has it initialized in one of local constructor. 
	 */
	protected ClientRegistrationPlugin registrationPlugin;
	
	/** Client registration class */
	protected RegistrationClass registrationClass;
	
	// SUBSCRIPTION + RECEPTION
	
	/** 
	 * CONSTANT : Reception inbound port URI, used as the component identifier within the Broker.
	 * Set in the Client constructor.
	 */
	public final String receivingInboundPortURI;
	
	/** Subscription + reception plug-in. */
	protected ClientSubscriptionPlugin subscriptionPlugin;
	
	// PUBLICATION
	
	/** Publication plug-in. */
	protected ClientPublicationPlugin publicationPlugin;
	
	// PRIVILEGED MANAGEMENT
	
	/** Privileged plug-in. */
	protected ClientPrivilegedPlugin privilegedPlugin;
	
	// -------------------------------------------------------------------------
	// Common plug-in setup methods
	// -------------------------------------------------------------------------
	
	/**
	 * Setup the {@code registrationPlugin} plug-in. Directly done in this class constructor, not accessible from outside.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true} // no precondition
	 * post	{@code this.registrationPlugin != null && this.registrationPlugin.isInitialised()}
	 * post	the Client has exactly 1 registration outbound port implementing RegistrationCI. // informal
	 * </pre>
	 * 
	 * @throws Exception 	If an error occurs while setuping the plug-in.
	 */
	private void setupRegistrationPlugin() throws Exception {
		// Install the registration plug-in.
		this.registrationPlugin = new ClientRegistrationPlugin();
		this.registrationPlugin.setPluginURI("registration");
		this.installPlugin(this.registrationPlugin);
		
		// Postcondition checks
		assert registrationPlugin.isInitialised() : 
			new PostconditionException("Registration plugin must be initialized");
		String[] registrationURIs = this.findOutboundPortURIsFromInterface(RegistrationCI.class);
		assert  registrationURIs.length == 1 : 
			new PostconditionException("The component must have an outbound port with implemented interface RegistrationCI");;
		assert	this.findPortFromURI(registrationURIs[0]).isPublished() :
			new PostconditionException("The component must have a RegistrationCI outbound port published");
	}
	
	/**
	 * Setup the {@code subscriptionPlugin} plug-in.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code this.registrationPlugin != null && this.registrationPlugin.isInitialised()}
	 * post	{@code this.subscriptionPlugin != null && this.subscriptionPlugin.isInitialised()}
	 * post	the Client has exactly 1 published inbound port implementing ReceivingCI. // informal
	 * </pre>
	 * 
	 * @throws Exception 	If an error occurs while setuping the plug-in.
	 */
	protected void setupSubscriptionPlugin() throws Exception {
		// Precondition check
		assert registrationPlugin != null && registrationPlugin.isInitialised() : // if initialized, registrationPlugin.getRegistrationOutboundPort() != null
			new PreconditionException("Registration plugin must be initialized.");
		
		// Install the subscription + reception plug-in
		this.subscriptionPlugin = new ClientSubscriptionPlugin(registrationPlugin);
		this.subscriptionPlugin.setPluginURI("subscription");
		this.installPlugin(this.subscriptionPlugin);
		
		// Postcondition checks
		assert this.subscriptionPlugin.isInitialised() : 
			new PostconditionException("Subscription (+reception) plugin must be initialized");
		String[] subscriptionURIs = this.findInboundPortURIsFromInterface(ReceivingCI.class);
		assert  subscriptionURIs.length == 1 : 
			new InvariantException("The component must have an inbound port with ReceivingCI implemented interface");
		assert	this.findPortFromURI(subscriptionURIs[0]).isPublished() :
			new InvariantException("The component must have a ReceivingCI inbound port published");
	}
	
	/**
	 * Setup the {@code publicationPlugin} plug-in + the inbound port implementing {@link AbnormalTerminationNotificationCI}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code this.registrationPlugin != null && this.registrationPlugin.isInitialised()}
	 * pre	{@code this.registrationPlugin.registered()}
	 * pre	{@code this.registrationPlugin.isInitialised()}
	 * post	{@code this.publicationPlugin != null && this.publicationPlugin.isInitialised()}
	 * post	the Client has exactly 1 published outbound port implementing PublishingCI. // informal
	 * </pre>
	 * 
	 * @throws Exception 	If an error occurs while setuping the plug-in.
	 */
	protected void setupPublicationPlugin() throws Exception {
		// Precondition checks
		assert registrationPlugin != null && registrationPlugin.isInitialised() : // if initialized, registrationPlugin.getRegistrationOutboundPort() != null
			new PreconditionException("Registration plugin must be initialized.");
		assert registrationPlugin.registered() : // if registered, registrationPlugin.getBrokerPublishingPortURI() != null
			new PreconditionException("Client component must be registered.");
		
		// Install the publication plug-in.
		this.publicationPlugin = new ClientPublicationPlugin(registrationPlugin);
		this.publicationPlugin.setPluginURI("publication");
		this.installPlugin(this.publicationPlugin);
		
		// Publication plug-in verification
		assert publicationPlugin.isInitialised() : 
			new InvariantException("Publication plugin must be initialized");
		String[] publicationURIs = this.findOutboundPortURIsFromInterface(PublishingCI.class);
		assert  publicationURIs.length == 1 : 
			new InvariantException("The component must have an outbound port with implemented interface PublishingCI");
		assert	this.findPortFromURI(publicationURIs[0]).isPublished() :
			new InvariantException("The component must have a PublishingCI outbound port published");
		
		// Abnormal termination verification
		String[] notificationURIs = this.findInboundPortURIsFromInterface(AbnormalTerminationNotificationCI.class);
		assert notificationURIs.length == 1 : 
			new InvariantException("The component must have an inbound port with implemented interface AbnormalTerminationNotificationCI");
		assert this.findPortFromURI(notificationURIs[0]).isPublished() :
			new InvariantException("The component must have a AbnormalTerminationNotificationCI inbound port published");
	}
	
	/**
	 * Setup the {@code privilegedPlugin} plug-in.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code this.registrationPlugin != null && this.registrationPlugin.isInitialised()}
	 * pre	{@code this.registrationPlugin.registered()}
	 * post	{@code this.privilegedPlugin != null && this.privilegedPlugin.isInitialised()}
	 * post	the Client has exactly 1 privileged outbound port implementing PrivilegedClientCI. // informal
	 * </pre>
	 * 
	 * @throws Exception 	If an error occurs while setuping the plug-in.
	 */
	protected void setupPrivilegedPlugin() throws Exception {
		// Precondition checks
		assert registrationPlugin != null && registrationPlugin.isInitialised() :
			new PreconditionException("Registration plugin must be initialized.");
		assert registrationPlugin.registered() : // if registered, registrationPlugin.getBrokerPublishingPortURI() != null
			new PreconditionException("Client component must be registered.");
		
		// Install the privileged plug-in.
		this.privilegedPlugin = new ClientPrivilegedPlugin(registrationPlugin.getBrokerPublishingPortURI());
		this.privilegedPlugin.setPluginURI("privileged");
		this.installPlugin(this.privilegedPlugin);
		
		// Privileged plug-in verification
		assert privilegedPlugin.isInitialised() : 
			new InvariantException("Privileged plugin must be initialized");
		String[] privilegedURIs = this.findOutboundPortURIsFromInterface(PrivilegedClientCI.class);
		assert  privilegedURIs.length == 1 : 
			new InvariantException("The component must have an outbound port with implemented interface PrivilegedClientCI");
		assert	this.findPortFromURI(privilegedURIs[0]).isPublished() :
			new InvariantException("The component must have a PrivilegedClientCI outbound port published");
	}
	
	// -------------------------------------------------------------------------
	// Constructor
	// -------------------------------------------------------------------------
	
	/**
	 * Creates a client component of the publish/subscribe system, by providing the {@code reflectionInboundPortURI} of the component.
	 * 
	 * <p>Used by test sub-classes.</p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code reflectionInboundPortURI != null && !reflectionInboundPortURI.isEmpty()}
	 * pre	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
	 * pre	{@code registrationClass != null}
	 * pre	{@code nbThreads >= 0 && nbSchedulableThreads >= 0}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param reflectionInboundPortURI		URI of the reflection inbound port to be created in this component.
	 * @param receivingInboundPortURI     	The URI of the inbound port offering {@link ReceivingCI}.
	 * @param registrationClass				The registration class of the component.
	 * @param nbThreads						Number of threads to be created in the component pool.
	 * @param nbSchedulableThreads			Number of threads to be created in the component schedulable pool.
	 * @throws Exception                  	If an error occurs during registration plugin installation.
	 */
	protected Client(String reflectionInboundPortURI, String receivingInboundPortURI, RegistrationClass registrationClass, 
			int nbThreads, int nbSchedulableThreads) throws Exception {
		super(reflectionInboundPortURI, nbThreads,nbSchedulableThreads);
		
		// Precondition checks
		assert(receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()) : 
			new PreconditionException("receivingInboundPortURI cannot be empty or null");
		assert(registrationClass != null) : 
			new PreconditionException("registrationClass cannot be empty or null");
		assert(nbThreads >= 0 && nbSchedulableThreads >= 0) : 
			new PreconditionException("cannot have a negative number of threads");
		assert(reflectionInboundPortURI != null && !reflectionInboundPortURI.isEmpty()) : 
			new PreconditionException("reflectionInboundPortURI cannot be empty or null");
		
		// Install the registration plug-in
		setupRegistrationPlugin();
		
		this.receivingInboundPortURI = receivingInboundPortURI;
		this.registrationClass = registrationClass;
	}
	
	@Override
	public void notifyAbnormalTermination(Class<? extends OfferedCI> offeredInterface, String inboundPortURI, 
			String serviceName, Serializable[] actualParameters,Throwable t) throws Exception {
		StringBuffer sb = new StringBuffer("Client::notifyAbnormalTermination ");
		sb.append(serviceName);
		sb.append("(");
		for (int i = 0 ; i < actualParameters.length ; i++) {
			sb.append(actualParameters[i]);
			if (i < actualParameters.length - 1) {
				sb.append(", ");
			}
		}
		sb.append(") call abnormally terminated with throwable ");
		sb.append(t.toString());
		System.err.println(sb.toString());
	}
}
