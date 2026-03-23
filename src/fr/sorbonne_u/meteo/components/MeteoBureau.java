package fr.sorbonne_u.meteo.components;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.clients.plugins.ClientPrivilegedPlugin;
import fr.sorbonne_u.clients.plugins.ClientPublicationPlugin;
import fr.sorbonne_u.clients.plugins.ClientSubscriptionPlugin;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.tasks_management.interfaces.AbnormalTerminationNotificationCI;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI;
import fr.sorbonne_u.cps.meteo.interfaces.RegionI;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.exceptions.PostconditionException;
import fr.sorbonne_u.exceptions.PreconditionException;
import fr.sorbonne_u.messages.Message;
import fr.sorbonne_u.meteo.data.MeteoAlert;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.meteo.data.RegionData;
import fr.sorbonne_u.meteo.tests.MeteoBureauTest;

/**
 * Class representing a meteorological office emitting random alerts periodically.
 * 
 * <p>
 * The office sends alerts at random intervals within the specified range, if indicated by the constructor used.
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code bureauPosition != null}
 * invariant	{@code sleepMaxTime >= sleepMinTime}
 * invariant	{@code publishingOutboundPortURI!= null & !publishingOutboundPortURI.isEmpty()}
 * invariant	if {@code this.rc != FREE} : {@code privilegedOutboundPortURI != null && !privilegedOutboundPortURI.isEmpty()}
 * invariant	{@code subscriptionPlugin != null && subscriptionPlugin.isInitialised()}
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class MeteoBureau extends Client {
	
	/** The component position */
	protected final PositionData bureauPosition;
	
	/** Minimum waiting time between each alert in seconds */
	protected int sleepMinTime;
	/** Maximum waiting time between each alert in seconds */
	protected int sleepMaxTime;
	
	/** 
	 * Indicates if the component must recursively send alerts while executing.<br>
	 * Classic constructor sets true, while the constructor used by test sub-classes sets false.
	 */
	private final boolean sendAlertsFromExec;
	
	// PUBLICATION
	
	/** Publication outbound port URI */
	protected final String publishingOutboundPortURI; 
	
	/** Publication plugin */
	protected ClientPublicationPlugin publicationPlugin;
	
	// SUBSCRIPTION + RECEPTION
	
	/** Subscription + reception plugin (used here only for subscription) */
	protected final ClientSubscriptionPlugin subscriptionPlugin;
	
	// PRIVILEGED MANAGEMENT
	
	/** Privileged outbound port URI, stays null if the client is not privileged */
	protected String privilegedOutboundPortURI = null;
	
	/** Privileged outbound port, stays null if the client is not privileged */
	protected ClientPrivilegedPlugin privilegedPlugin = null;

	//--------------------------------------------------------------------------
	// Constructors
	//--------------------------------------------------------------------------

	/**
	 * Creates a meteorological office component of the publish/subscribe system, extending {@link Client}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
	 * pre	{@code registrationOutboundPortURI != null && !registrationOutboundPortURI.isEmpty()}
	 * pre	{@code publishingOutboundPortURI != null && !publishingOutboundPortURI.isEmpty()}
	 * pre	{@code privilegedOutboundPortURI != null}, if {@code rc != FREE : !privilegedOutboundPortURI.isEmpty()}
	 * pre	{@code nbThreads >= 0 && nbSchedulableThreads >= 0}
	 * pre	{@code bureauPosition != null}
	 * pre	{@code sleepMinTime >= 0 && sleepMaxTime >= sleepMinTime}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param receivingInboundPortURI       URI of the inbound port offering {@link ReceivingCI}.
	 * @param registrationOutboundPortURI   URI of the outbound port requiring {@link RegistrationCI}.
	 * @param publishingOutboundPortURI     URI of the outbound port requiring {@link PublishingCI}.
	 * @param privilegedOutboundPortURI	    URI of the outbound port requiring {@link PrivilegedClientCI}. Can be empty if the client if FREE, otherwise it cannot.
	 * @param rc			   				The registration class of the component.
	 * @param nbThreads						Number of threads to be created in the component pool.
	 * @param nbSchedulableThreads			Number of threads to be created in the component schedulable pool.
	 * @param bureauPosition                The position of the office.
	 * @param sleepMinTime                  Minimum waiting time between two alerts.
	 * @param sleepMaxTime                  Maximum waiting time between two alerts.
	 * @throws Exception                    If an error occurs during initialization or port publication.
	 */
	protected MeteoBureau(String receivingInboundPortURI, String registrationOutboundPortURI, String publishingOutboundPortURI, String privilegedOutboundPortURI, 
			RegistrationClass rc, int nbThreads, int nbSchedulableThreads, PositionData bureauPosition, int sleepMinTime, int sleepMaxTime) throws Exception {
		super(receivingInboundPortURI, registrationOutboundPortURI, rc, nbThreads, nbSchedulableThreads);
		
		// Some preconditions are checked inside the superconstructor while others are checked here
		assert(bureauPosition != null) : new PreconditionException("bureauPosition cannot be null");
		assert(sleepMinTime >= 0 && sleepMaxTime >= sleepMinTime) : new PreconditionException("sleep times must be coherent");
		assert(privilegedOutboundPortURI != null) : new PreconditionException("privilegedOutboundPortURI cannot be null");
		if (rc != RegistrationClass.FREE) {
			assert (!privilegedOutboundPortURI.isEmpty()) : new PreconditionException("privilegedOutboundPortURI cannot be empty if the client is privileged");
			this.privilegedOutboundPortURI = privilegedOutboundPortURI;
		}
		
		this.bureauPosition = bureauPosition;
		this.sleepMinTime = sleepMinTime;
		this.sleepMaxTime = sleepMaxTime;
		this.publishingOutboundPortURI = publishingOutboundPortURI;
		this.sendAlertsFromExec = true;
		
		// Install the subscription + reception plugin
		this.subscriptionPlugin = new ClientSubscriptionPlugin(receivingInboundPortURI, registrationPlugin.getRegistrationOutboundPort());
		this.subscriptionPlugin.setPluginURI("subscription");
		this.installPlugin(this.subscriptionPlugin);
		
		// Invariant checks
		AbstractComponent.checkImplementationInvariant(this);
		AbstractComponent.checkInvariant(this);
		
		// Postcondition checks
		assert subscriptionPlugin.isInitialised() : 
			new PostconditionException("Subscription (+reception) plugin must be initialized");
		assert	this.isPortExisting(receivingInboundPortURI) : 
			new PostconditionException("The component must have a port with URI " + receivingInboundPortURI);
		assert	this.findPortFromURI(receivingInboundPortURI).getImplementedInterface().equals(ReceivingCI.class) : 
			new PostconditionException("The component must have a port with implemented interface ReceivingCI");
		assert	this.findPortFromURI(receivingInboundPortURI).isPublished() :
			new PostconditionException("The component must have a port published with URI " + receivingInboundPortURI);
		
		this.getTracer().setTitle("MeteoBureau");
		this.getTracer().setRelativePosition((int)bureauPosition.getPosX(), (int)bureauPosition.getPosY());
	}
	
	/**
	 * Creates a meteorological office component of the publish/subscribe system, extending {@link Client}, by providing 
	 * the {@code reflectionInboundPortURI} of the component.
	 * 
	 * <p>Constructor used by test sub-class {@link MeteoBureauTest}.</p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
	 * pre	{@code registrationOutboundPortURI != null && !registrationOutboundPortURI.isEmpty()}
	 * pre	{@code publishingOutboundPortURI != null && !publishingOutboundPortURI.isEmpty()}
	 * pre	{@code privilegedOutboundPortURI != null}, if {@code rc != FREE : !privilegedOutboundPortURI.isEmpty()}
	 * pre	{@code nbThreads >= 0 && nbSchedulableThreads >= 0}
	 * pre	{@code bureauPosition != null}
	 * pre	{@code reflectionInboundPortURI != null && !reflectionInboundPortURI.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param receivingInboundPortURI       URI of the inbound port offering {@link ReceivingCI}.
	 * @param registrationOutboundPortURI   URI of the outbound port requiring {@link RegistrationCI}.
	 * @param publishingOutboundPortURI     URI of the outbound port requiring {@link PublishingCI}.
	 * @param privilegedOutboundPortURI	    URI of the outbound port requiring {@link PrivilegedClientCI}. Can be empty if the client if FREE, otherwise it cannot.
	 * @param rc			   				The registration class of the component.
	 * @param nbThreads						Number of threads to be created in the component pool.
	 * @param nbSchedulableThreads			Number of threads to be created in the component schedulable pool.
	 * @param bureauPosition                The position of the office.
	 * @param reflectionInboundPortURI		URI of the reflection inbound port to be created in this component.
	 * @throws Exception                    If an error occurs during initialization or port publication.
	 */
	protected MeteoBureau(String receivingInboundPortURI, String registrationOutboundPortURI, String publishingOutboundPortURI, String privilegedOutboundPortURI, 
			RegistrationClass rc, int nbThreads, int nbSchedulableThreads, PositionData bureauPosition, String reflectionInboundPortURI) throws Exception {
		super(receivingInboundPortURI, registrationOutboundPortURI, rc, nbThreads, nbSchedulableThreads, reflectionInboundPortURI);
		
		// Some preconditions are checked inside the superconstructor while others are checked here
		assert(bureauPosition != null) : new PreconditionException("bureauPosition cannot be null");
		assert(sleepMinTime >= 0 && sleepMaxTime >= sleepMinTime) : new PreconditionException("sleep times must be coherent");
		assert(privilegedOutboundPortURI != null) : new PreconditionException("privilegedOutboundPortURI cannot be null");
		if (rc != RegistrationClass.FREE) {
			assert (!privilegedOutboundPortURI.isEmpty()) : new PreconditionException("privilegedOutboundPortURI cannot be empty if the client is privileged");
			this.privilegedOutboundPortURI = privilegedOutboundPortURI;
		}
		
		this.bureauPosition = bureauPosition;
		this.sleepMinTime = -1;
		this.sleepMaxTime = -1;
		this.publishingOutboundPortURI = publishingOutboundPortURI;
		this.sendAlertsFromExec = false;
		
		// Install the subscription + reception plugin
		this.subscriptionPlugin = new ClientSubscriptionPlugin(receivingInboundPortURI, registrationPlugin.getRegistrationOutboundPort());
		this.subscriptionPlugin.setPluginURI("subscription");
		this.installPlugin(this.subscriptionPlugin);
		
		// Invariant checks
		AbstractComponent.checkImplementationInvariant(this);
		AbstractComponent.checkInvariant(this);
		
		// Postcondition checks
		assert subscriptionPlugin.isInitialised() : 
			new PostconditionException("Subscription (+reception) plugin must be initialized");
		assert	this.isPortExisting(receivingInboundPortURI) : 
			new PostconditionException("The component must have a port with URI " + receivingInboundPortURI);
		assert	this.findPortFromURI(receivingInboundPortURI).getImplementedInterface().equals(ReceivingCI.class) : 
			new PostconditionException("The component must have a port with implemented interface ReceivingCI");
		assert	this.findPortFromURI(receivingInboundPortURI).isPublished() :
			new PostconditionException("The component must have a port published with URI " + receivingInboundPortURI);
		
		this.getTracer().setTitle("MeteoBureau");
		this.getTracer().setRelativePosition((int)bureauPosition.getPosX(), (int)bureauPosition.getPosY());
	}

	// --------------------------------------------------------------------------
	// Component life-cycle
	// --------------------------------------------------------------------------

	@Override
	public void execute() throws Exception {
		super.execute();
		this.logMessage("executing MeteoBureau component");
		
		// Register on the Broker
		registrationPlugin.register(registrationClass);
		assert (registrationPlugin.getBrokerPublishingPortURI() != null);
		this.logMessage("MeteoBureau registered");
		
		// Install the publication plug-in.
		this.publicationPlugin = new ClientPublicationPlugin(receivingInboundPortURI, registrationPlugin.getRegistrationOutboundPort(),
				publishingOutboundPortURI, registrationPlugin.getBrokerPublishingPortURI());
		this.publicationPlugin.setPluginURI("publication");
		this.installPlugin(this.publicationPlugin);
		
		assert publicationPlugin.isInitialised();
		assert this.isPortExisting(publishingOutboundPortURI);
		assert this.findPortFromURI(publishingOutboundPortURI).getImplementedInterface().equals(PublishingCI.class);
		assert this.findPortFromURI(publishingOutboundPortURI).isPublished();
		String[] notificationURIs = this.findInboundPortURIsFromInterface(AbnormalTerminationNotificationCI.class);
		assert notificationURIs.length == 1;
		assert this.findPortFromURI(notificationURIs[0]).isPublished();
		
		// Install the privileged plug-in if it is allowed.
		if (this.registrationClass == RegistrationClass.STANDARD || this.registrationClass == RegistrationClass.PREMIUM) {
			this.privilegedPlugin = new ClientPrivilegedPlugin(receivingInboundPortURI, privilegedOutboundPortURI, registrationPlugin.getBrokerPublishingPortURI());
			this.privilegedPlugin.setPluginURI("privileged");
			this.installPlugin(this.privilegedPlugin);
			
			assert privilegedPlugin.isInitialised();
			assert this.isPortExisting(privilegedOutboundPortURI);
			assert this.findPortFromURI(privilegedOutboundPortURI).getImplementedInterface().equals(PrivilegedClientCI.class);
			assert this.findPortFromURI(privilegedOutboundPortURI).isPublished();
		}
		
		if (sendAlertsFromExec) publishRandomAlert("channel1");
	}
	
	// --------------------------------------------------------------------------
	// Alert sending
	// --------------------------------------------------------------------------

	/**
	 * Sends a {@link MeteoAlert} with random attributes.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param channel      The name of the channel to send the alert on.
	 * @throws Exception   If an error occurs during publishing.
	 */
	protected void publishRandomAlert(String channel) throws Exception {
		// Precondition check
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");	
		
		int seconds = ThreadLocalRandom.current().nextInt(sleepMinTime, sleepMaxTime); 
		this.logMessage("MeteoBureau sleeps "+seconds+" seconds..."); 
		Thread.sleep(seconds * 1000L);
		
		Message msg = new Message();
		
		ArrayList<RegionI> regions = new ArrayList<>();
		regions.add(randomRegion());
		
		MeteoAlertI.AlertType alert = randomAlert();
		MeteoAlertI.Level level = randomLevel();
		Instant instant = Instant.now();
		Duration duration = randomDuration();
		MeteoAlert ma = new MeteoAlert(alert, level, regions, instant, duration);
		
		msg.setPayload(ma);
		msg.putProperty("alert", alert);
		msg.putProperty("level", level);
		msg.putProperty("regions", regions);
		msg.putProperty("instant", instant);
		msg.putProperty("duration", duration);
			
		this.logMessage("------------------------------");
		this.logMessage("MeteoBureau sending an alert "+ma);
		this.logMessage("------------------------------");
		
		publicationPlugin.publish(channel, msg);
		
		// Recursive call
		publishRandomAlert("channel1");
	}
	
	// --------------------------------------------------------------------------
	// Internal random helpers
	// --------------------------------------------------------------------------

	/**
	 * Creates a random alert type.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @return a random alert type.
	 */
	protected MeteoAlertI.AlertType randomAlert() {
		switch (ThreadLocalRandom.current().nextInt(0, 6)) {
	        case 0: return MeteoAlertI.AlertType.STORM;
	        case 1: return MeteoAlertI.AlertType.ICY_STORM;
	        case 2: return MeteoAlertI.AlertType.SNOW_STORM;
	        case 3: return MeteoAlertI.AlertType.FLOODING;
	        case 4: return MeteoAlertI.AlertType.EARTHQUAKE;
	        case 5: return MeteoAlertI.AlertType.ERUPTION;
	        default: throw new AssertionError("Impossible");
	    }
    }
	
	/**
	 * Creates a random alert level.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @return a random alert level.
	 */
	protected MeteoAlertI.Level randomLevel() {
		switch (ThreadLocalRandom.current().nextInt(0, 5)) {
	        case 0: return MeteoAlertI.Level.GREEN;
	        case 1: return MeteoAlertI.Level.YELLOW;
	        case 2: return MeteoAlertI.Level.ORANGE;
	        case 3: return MeteoAlertI.Level.RED;
	        case 4: return MeteoAlertI.Level.SCARLET;
	        default: throw new AssertionError("Impossible");
	    }
    }
	
	/**
	 * Creates a random {@link RegionData}.
	 * 
	 * <p>The region created has a 3 x 3 size, with minimum X/Y in [0, 6[.</p> 
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @return a random {@link RegionData}.
	 */
	protected RegionData randomRegion() {
		double xMin = ThreadLocalRandom.current().nextInt(0, 6);
		double xMax = xMin + 3;
		double yMin = ThreadLocalRandom.current().nextInt(0, 6);
		double yMax = yMin + 3;
		return new RegionData(xMin, xMax, yMin, yMax);
    }

	/**
	 * Creates a random {@link Duration}.
	 * 
	 * <p>The duration is between 5 and 60 seconds</p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @return a random {@link Duration}.
	 */
	protected Duration randomDuration() {
	    int seconds = ThreadLocalRandom.current().nextInt(5, 61);
	    return Duration.ofSeconds(seconds);
	}
}