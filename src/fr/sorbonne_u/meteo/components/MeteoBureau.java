package fr.sorbonne_u.meteo.components;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI;
import fr.sorbonne_u.cps.meteo.interfaces.RegionI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.exceptions.PreconditionException;
import fr.sorbonne_u.messages.Message;
import fr.sorbonne_u.messages.meteo.AlertMessage;
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
 * invariant	{@code this.subscriptionPlugin != null && this.subscriptionPlugin.isInitialised()}
 * invariant	the Client has exactly 1 published inbound port implementing ReceivingCI. // informal
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

	//--------------------------------------------------------------------------
	// Constructors
	//--------------------------------------------------------------------------

	/**
	 * Creates a meteorological office component of the publish/subscribe system, extending {@link Client}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
	 * pre	{@code nbThreads >= 0 && nbSchedulableThreads >= 0}
	 * pre	{@code bureauPosition != null}
	 * pre	{@code sleepMinTime >= 0 && sleepMaxTime >= sleepMinTime}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param receivingInboundPortURI       URI of the inbound port offering {@link ReceivingCI}.
	 * @param rc			   				The registration class of the component.
	 * @param nbThreads						Number of threads to be created in the component pool.
	 * @param nbSchedulableThreads			Number of threads to be created in the component schedulable pool.
	 * @param bureauPosition                The position of the office.
	 * @param sleepMinTime                  Minimum waiting time between two alerts.
	 * @param sleepMaxTime                  Maximum waiting time between two alerts.
	 * @throws Exception                    If an error occurs during initialization or port publication.
	 */
	protected MeteoBureau(String receivingInboundPortURI, 
			RegistrationClass rc, int nbThreads, int nbSchedulableThreads, PositionData bureauPosition, int sleepMinTime, int sleepMaxTime) throws Exception {
		super(receivingInboundPortURI, rc, nbThreads, nbSchedulableThreads);
		
		// Some preconditions are checked inside the super-constructor while others are checked here
		assert(bureauPosition != null) : 
			new PreconditionException("bureauPosition cannot be null");
		assert(sleepMinTime >= 0 && sleepMaxTime >= sleepMinTime) : 
			new PreconditionException("sleep times must be coherent");
		
		this.bureauPosition = bureauPosition;
		this.sleepMinTime = sleepMinTime;
		this.sleepMaxTime = sleepMaxTime;
		this.sendAlertsFromExec = true;
		
		// Install the subscription + default reception plug-in
		setupSubscriptionPlugin();
		
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
	 * pre	{@code nbThreads >= 0 && nbSchedulableThreads >= 0}
	 * pre	{@code bureauPosition != null}
	 * pre	{@code reflectionInboundPortURI != null && !reflectionInboundPortURI.isEmpty()}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param receivingInboundPortURI       URI of the inbound port offering {@link ReceivingCI}.
	 * @param rc			   				The registration class of the component.
	 * @param nbThreads						Number of threads to be created in the component pool.
	 * @param nbSchedulableThreads			Number of threads to be created in the component schedulable pool.
	 * @param bureauPosition                The position of the office.
	 * @param reflectionInboundPortURI		URI of the reflection inbound port to be created in this component.
	 * @throws Exception                    If an error occurs during initialization or port publication.
	 */
	protected MeteoBureau(String receivingInboundPortURI, 
			RegistrationClass rc, int nbThreads, int nbSchedulableThreads, PositionData bureauPosition, String reflectionInboundPortURI) throws Exception {
		super(receivingInboundPortURI, rc, nbThreads, nbSchedulableThreads, reflectionInboundPortURI);
		
		// Some preconditions are checked inside the super-constructor while others are checked here
		assert(bureauPosition != null) : 
			new PreconditionException("bureauPosition cannot be null");
		assert(sleepMinTime >= 0 && sleepMaxTime >= sleepMinTime) : 
			new PreconditionException("sleep times must be coherent");
		
		// Install the subscription + default reception plug-in
		setupSubscriptionPlugin();
		
		this.bureauPosition = bureauPosition;
		this.sleepMinTime = -1;
		this.sleepMaxTime = -1;
		this.sendAlertsFromExec = false;
		
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
		
		// Install the publication plug-in
		setupPublicationPlugin();
		
		// Install the privileged plug-in if it is allowed.
		if (this.registrationClass == RegistrationClass.STANDARD || this.registrationClass == RegistrationClass.PREMIUM) {
			setupPrivilegedPlugin();
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
		
		ArrayList<RegionI> regions = new ArrayList<>();
		regions.add(randomRegion());
		
		MeteoAlertI.AlertType alert = randomAlert();
		MeteoAlertI.Level level = randomLevel();
		Instant instant = Instant.now();
		Duration duration = randomDuration();
		MeteoAlert ma = new MeteoAlert(alert, level, regions, instant, duration);
		Message msg = new AlertMessage(ma);
			
		this.logMessage("------------------------------\n"
				+ "MeteoBureau sending an alert "+ma
				+ "\n------------------------------");
		
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
	        default: throw new IllegalArgumentException("Impossible alert type");
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
	        default: throw new IllegalArgumentException("Impossible alert level");
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