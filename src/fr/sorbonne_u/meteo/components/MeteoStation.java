package fr.sorbonne_u.meteo.components;

import java.util.concurrent.ThreadLocalRandom;

import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.clients.plugins.ClientPrivilegedPlugin;
import fr.sorbonne_u.clients.plugins.ClientPublicationPlugin;
import fr.sorbonne_u.clients.plugins.ClientSubscriptionPlugin;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.exceptions.PostconditionException;
import fr.sorbonne_u.exceptions.PreconditionException;
import fr.sorbonne_u.messages.Message;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.meteo.data.WindData;

/**
 * Class representing a meteorological station emitting random wind data periodically.
 * 
 * <p>
 * The station generates wind vectors within a specified range for X and Y components.
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code sleepMinTime >= 0}
 * invariant	{@code sleepMaxTime >= sleepMinTime}
 * invariant	{@code publishingOutboundPortURI!= null & !publishingOutboundPortURI.isEmpty()}
 * invariant	if {@code this.rc != FREE} : {@code privilegedOutboundPortURI != null && !privilegedOutboundPortURI.isEmpty()}
 * invariant	{@code subscriptionPlugin != null && subscriptionPlugin.isInitialised()}
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class MeteoStation extends Client {
	
	private PositionData stationPosition;

	// Waiting time between each WindData in seconds
	private int sleepMinTime;
	private int sleepMaxTime;
	
	// Bounds for generating X and Y components of wind vectors
	private double windMinXY;
	private double windMaxXY;
	
	// Publication
	protected final String publishingOutboundPortURI;
	protected ClientPublicationPlugin publicationPlugin;
	
	// Subscription + reception
	protected final ClientSubscriptionPlugin subscriptionPlugin;
	
	// Privileged management
	protected String privilegedOutboundPortURI = null; // stays null if the client is not privileged
	protected ClientPrivilegedPlugin privilegedPlugin = null; // stays null if the client is not privileged
	
	// --------------------------------------------------------------------------
	// Constructor
	// --------------------------------------------------------------------------
	
	/**
	 * Creates a meteorological station component for the publish/subscribe system, extending a {@link Client}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre  {@code registrationOutboundPortURI != null && !registrationOutboundPortURI.isEmpty()}
	 * pre  {@code publishingOutboundPortURI != null && !publishingOutboundPortURI.isEmpty()}
	 * pre  {@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
	 * pre	{@code privilegedOutboundPortURI != null}, if {@code rc != FREE : !privilegedOutboundPortURI.isEmpty()}
	 * pre	{@code nbThreads >= 0 && nbSchedulableThreads >= 0}
	 * pre  {@code stationPosition != null}
	 * pre  {@code sleepMinTime >= 0 && sleepMaxTime >= sleepMinTime}
	 * pre  {@code windMaxXY >= windMinXY}
	 * post {@code true} // no postcondition.
	 * </pre>
	 * 
	 * @param registrationOutboundPortURI URI of the outbound port requesting {@link RegistrationCI}.
	 * @param publishingOutboundPortURI   URI of the outbound port requesting {@link PublishingCI}.
	 * @param receivingInboundPortURI     URI of the inbound port providing {@link ReceivingCI}.
	 * @param privilegedOutboundPortURI	  URI of the outbound port requiring {@link PrivilegedClientCI}. Can be empty if the client if FREE, otherwise it cannot.
	 * @param rc			  		 	  The registration class of the component.
	 * @param nbThreads					  Number of threads to be created in the component pool.
	 * @param nbSchedulableThreads		  Number of threads to be created in the component schedulable pool.
	 * @param stationPosition             The position of the station.
	 * @param sleepMinTime                Minimum wait time between each new {@link WindData} sending.
	 * @param sleepMaxTime                Maximum wait time between each new {@link WindData} sending.
	 * @param windMinXY                   Minimum X value for a new {@link WindData}.
	 * @param windMaxXY                   Maximum X value for a new {@link WindData}.
	 * @throws Exception                  If a problem occurs during initialization or port publication.
	 */
	protected MeteoStation(String registrationOutboundPortURI, String publishingOutboundPortURI, String receivingInboundPortURI, String privilegedOutboundPortURI, 
			RegistrationClass rc, int nbThreads, int nbSchedulableThreads, PositionData stationPosition, int sleepMinTime, int sleepMaxTime, double windMinXY, double windMaxXY) throws Exception {
		super(receivingInboundPortURI, registrationOutboundPortURI, rc, nbThreads, nbSchedulableThreads);
		// Some preconditions are checked inside the superconstructor while others are checked here
		assert(stationPosition != null) : new PreconditionException("stationPosition cannot be null");	
		assert(sleepMinTime >= 0 && sleepMaxTime >= sleepMinTime): new PreconditionException("sleep times must be coherent");
		assert(windMaxXY >= windMinXY): new PreconditionException("wind range must be coherent");
		assert(privilegedOutboundPortURI != null) : new PreconditionException("privilegedOutboundPortURI cannot be null");
		if (rc != RegistrationClass.FREE) {
			assert (!privilegedOutboundPortURI.isEmpty()) : new PreconditionException("privilegedOutboundPortURI cannot be empty if the client is privileged");
			this.privilegedOutboundPortURI = privilegedOutboundPortURI;
		}

		this.stationPosition = stationPosition;
		this.sleepMinTime = sleepMinTime;
		this.sleepMaxTime = sleepMaxTime;
		this.windMinXY = windMinXY;
		this.windMaxXY = windMaxXY;
		this.publishingOutboundPortURI = publishingOutboundPortURI;
		
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
		
		this.getTracer().setTitle("MeteoStation");
		this.getTracer().setRelativePosition((int)stationPosition.getPosX(), (int)stationPosition.getPosY());
	}
	
	// --------------------------------------------------------------------------
	// Component life-cycle
	// --------------------------------------------------------------------------

	@Override
	public void execute() throws Exception {
		super.execute();
		this.logMessage("executing MeteoStation component");
		
		// Register on the Broker
		registrationPlugin.register(registrationClass);
		assert (registrationPlugin.getBrokerPublishingPortURI() != null);
		this.logMessage("MeteoStation registered");
		
		// Install the publication plug-in.
		this.publicationPlugin = new ClientPublicationPlugin(receivingInboundPortURI, registrationPlugin.getRegistrationOutboundPort(),
				publishingOutboundPortURI, registrationPlugin.getBrokerPublishingPortURI());
		this.publicationPlugin.setPluginURI("publication");
		this.installPlugin(this.publicationPlugin);
		assert publicationPlugin.isInitialised();
		
		// Install the privileged plug-in if it is allowed.
		if (this.registrationClass == RegistrationClass.STANDARD || this.registrationClass == RegistrationClass.PREMIUM) {
			this.privilegedPlugin = new ClientPrivilegedPlugin(receivingInboundPortURI, privilegedOutboundPortURI, registrationPlugin.getBrokerPublishingPortURI());
			this.privilegedPlugin.setPluginURI("privileged");
			this.installPlugin(this.privilegedPlugin);
			assert privilegedPlugin.isInitialised();
		}
		
		sendWindData("channel0");
	}
	
	// --------------------------------------------------------------------------
	// Periodic sending
	// --------------------------------------------------------------------------
	
	/**
	 * Sends a {@link WindData} with random X/Y components in the range [windMinXY, windMaxXY].
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 */
	protected void sendWindData(String channel) throws Exception {
		// Precondition check
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");	
		
		int seconds = ThreadLocalRandom.current().nextInt(sleepMinTime, sleepMaxTime); 
		this.logMessage("MeteoStation sleeps "+seconds+" seconds..."); 
		Thread.sleep(seconds * 1000L);
		
		Message msg = new Message();
		double xWind = ThreadLocalRandom.current().nextDouble(windMinXY, windMaxXY);
		double yWind = ThreadLocalRandom.current().nextDouble(windMinXY, windMaxXY);
		WindData wd = new WindData(stationPosition, xWind, yWind);
		
		msg.setPayload(wd);
		msg.putProperty("position", stationPosition);
		msg.putProperty("xWind", xWind);
		msg.putProperty("yWind", yWind);
		
		this.logMessage("------------------------------");
		this.logMessage("MeteoStation sending wind data "+wd);
		this.logMessage("------------------------------");
		
		publicationPlugin.publish(channel, msg);
		
		// Recursive call
		sendWindData("channel0");
	}
}