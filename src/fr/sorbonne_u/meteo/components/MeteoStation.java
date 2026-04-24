package fr.sorbonne_u.meteo.components;

import java.util.concurrent.ThreadLocalRandom;

import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.exceptions.PreconditionException;
import fr.sorbonne_u.messages.Message;
import fr.sorbonne_u.messages.meteo.WindMessage;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.meteo.data.WindData;
import fr.sorbonne_u.meteo.tests.MeteoStationTest;

/**
 * Class representing a meteorological station emitting random wind data periodically.
 * 
 * <p>
 * The station generates wind vectors within a specified range for X and Y components.
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code sleepMaxTime >= sleepMinTime}
 * invariant	{@code this.subscriptionPlugin != null && this.subscriptionPlugin.isInitialised()}
 * invariant	the Client has exactly 1 published inbound port implementing ReceivingCI. // informal
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class MeteoStation extends Client {
	
	/** The component position */
	protected final PositionData stationPosition;

	/** Minimum waiting time between each wind data sending in seconds */
	protected int sleepMinTime;
	/** Maximum waiting time between each wind data sending in seconds */
	protected int sleepMaxTime;
	
	/** Lower bound for generating X and Y components of wind vectors */
	protected double windMinXY;
	/** Upper bound for generating X and Y components of wind vectors */
	protected double windMaxXY;
	
	/** 
	 * Indicates if the component must recursively send wind datas while executing.<br>
	 * Classic constructor sets true, while the constructor used by test sub-classes sets false.
	 */
	private final boolean sendWDFromExec;
	
	// --------------------------------------------------------------------------
	// Constructor
	// --------------------------------------------------------------------------
	
	/**
	 * Creates a meteorological station component for the publish/subscribe system, extending a {@link Client}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
	 * pre	{@code nbThreads >= 0 && nbSchedulableThreads >= 0}
	 * pre  {@code stationPosition != null}
	 * pre  {@code sleepMinTime >= 0 && sleepMaxTime >= sleepMinTime}
	 * pre  {@code windMaxXY >= windMinXY}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param receivingInboundPortURI       URI of the inbound port offering {@link ReceivingCI}.
	 * @param rc			  		 	  	The registration class of the component.
	 * @param nbThreads					  	Number of threads to be created in the component pool.
	 * @param nbSchedulableThreads		  	Number of threads to be created in the component schedulable pool.
	 * @param stationPosition             	The position of the station.
	 * @param sleepMinTime                	Minimum wait time between each new {@link WindData} sending.
	 * @param sleepMaxTime                	Maximum wait time between each new {@link WindData} sending.
	 * @param windMinXY                   	Minimum X value for a new {@link WindData}.
	 * @param windMaxXY                   	Maximum X value for a new {@link WindData}.
	 * @throws Exception                  	If a problem occurs during initialization or port publication.
	 */
	protected MeteoStation(String receivingInboundPortURI, 
			RegistrationClass rc, int nbThreads, int nbSchedulableThreads, PositionData stationPosition, int sleepMinTime, int sleepMaxTime, double windMinXY, double windMaxXY) throws Exception {
		super(receivingInboundPortURI, rc, nbThreads, nbSchedulableThreads);
		
		// Some preconditions are checked inside the super-constructor while others are checked here
		assert(stationPosition != null) : 
			new PreconditionException("stationPosition cannot be null");	
		assert(sleepMinTime >= 0 && sleepMaxTime >= sleepMinTime): 
			new PreconditionException("sleep times must be coherent");
		assert(windMaxXY >= windMinXY): 
			new PreconditionException("wind range must be coherent");

		this.stationPosition = stationPosition;
		this.sleepMinTime = sleepMinTime;
		this.sleepMaxTime = sleepMaxTime;
		this.windMinXY = windMinXY;
		this.windMaxXY = windMaxXY;
		this.sendWDFromExec = true;
		
		// Install the subscription + default reception plug-in
		setupSubscriptionPlugin();
		
		this.getTracer().setTitle("MeteoStation");
		this.getTracer().setRelativePosition((int)stationPosition.getPosX(), (int)stationPosition.getPosY());
	}
	
	/**
	 * Creates a meteorological station component for the publish/subscribe system, extending a {@link Client}, by providing 
	 * the {@code reflectionInboundPortURI} of the component.
	 * 
	 * <p>Constructor used by test sub-class {@link MeteoStationTest}.</p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
	 * pre	{@code nbThreads >= 0 && nbSchedulableThreads >= 0}
	 * pre  {@code stationPosition != null}
	 * pre	{@code reflectionInboundPortURI != null && !reflectionInboundPortURI.isEmpty()}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param receivingInboundPortURI       URI of the inbound port offering {@link ReceivingCI}.
	 * @param rc			  		 	  	The registration class of the component.
	 * @param nbThreads					  	Number of threads to be created in the component pool.
	 * @param nbSchedulableThreads		  	Number of threads to be created in the component schedulable pool.
	 * @param stationPosition             	The position of the station.
	 * @param reflectionInboundPortURI		URI of the reflection inbound port to be created in this component.
	 * @throws Exception                  	If a problem occurs during initialization or port publication.
	 */
	protected MeteoStation(String receivingInboundPortURI, 
			RegistrationClass rc, int nbThreads, int nbSchedulableThreads, PositionData stationPosition, String reflectionInboundPortURI) throws Exception {
		super(receivingInboundPortURI, rc, nbThreads, nbSchedulableThreads, reflectionInboundPortURI);
		
		// Some preconditions are checked inside the super-constructor while others are checked here
		assert(stationPosition != null) : 
			new PreconditionException("stationPosition cannot be null");	
		assert(sleepMinTime >= 0 && sleepMaxTime >= sleepMinTime): 
			new PreconditionException("sleep times must be coherent");
		assert(windMaxXY >= windMinXY): 
			new PreconditionException("wind range must be coherent");

		this.stationPosition = stationPosition;
		this.sleepMinTime = -1;
		this.sleepMaxTime = -1;
		this.windMinXY = 0;
		this.windMaxXY = 0;
		this.sendWDFromExec = false;
		
		// Install the subscription + default reception plug-in
		setupSubscriptionPlugin();
		
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
		
		// Install the publication plug-in
		setupPublicationPlugin();
		
		// Install the privileged plug-in if it is allowed.
		if (this.registrationClass == RegistrationClass.STANDARD || this.registrationClass == RegistrationClass.PREMIUM) {
			setupPrivilegedPlugin();
		}
		
		if (sendWDFromExec) sendRandomWindData("channel0");
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
	 * @param channel 		The channel to which the wind will be sent.
	 * @throws Exception	if an error occurs wile publishing the wind data.
	 */
	protected void sendRandomWindData(String channel) throws Exception {
		// Precondition check
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");	
		
		int seconds = ThreadLocalRandom.current().nextInt(sleepMinTime, sleepMaxTime); 
		this.logMessage("MeteoStation sleeps "+seconds+" seconds..."); 
		Thread.sleep(seconds * 1000L);
		
		double xWind = ThreadLocalRandom.current().nextDouble(windMinXY, windMaxXY);
		double yWind = ThreadLocalRandom.current().nextDouble(windMinXY, windMaxXY);
		WindData wd = new WindData(stationPosition, xWind, yWind);
		Message msg = new WindMessage(wd);
		
		this.logMessage("------------------------------\n"
				+ "MeteoStation sending wind data "+wd
				+ "\n------------------------------");
		
		publicationPlugin.publish(channel, msg);
		
		// Recursive call
		sendRandomWindData("channel0");
	}
}