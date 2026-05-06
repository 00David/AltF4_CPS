package fr.sorbonne_u.meteo.components;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.Future;

import fr.sorbonne_u.clients.component.Client;
import fr.sorbonne_u.components.utils.tests.TestScenario;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI.LevelI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.exceptions.PreconditionException;
import fr.sorbonne_u.meteo.data.MeteoAlert;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.meteo.data.WindData;
import fr.sorbonne_u.meteo.plugins.WindTurbineReceptionPlugin;
import fr.sorbonne_u.utils.aclocks.ClocksServer;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.exceptions.InvariantException;
import fr.sorbonne_u.exceptions.PostconditionException;

/**
 * The WindTurbine component, {@link Client} of the system. Can participate in a given test scenario with the correct constructor.
 * 
 * <p>
 * It receives messages sent by meteorological stations/offices and takes
 * appropriate decisions for its orientation or safety mode.
 * </p>
 * 
 * <p>
 * Multi-tread safeness is guaranteed by the usage of 2 locks :
 * 	<ul>
 * 		<li>The {@code datas} reference lock, when accessing the list of considered wind datas for the orientation computation.</li>
 * 		<li>The {@code safeModeLock}, for accessing the current state of {@code safeMode} .</li>
 * 	</ul>
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code windTurbinePos != null}
 * invariant	{@code filterChannel0 != null}
 * invariant	{@code filterChannel1 != null}
 * invariant	{@code orientation != null}
 * invariant	{@code maxWorkingLevel != null}
 * invariant	{@code datas != null}
 * invariant	{@code safeModeLock != null}
 * invariant	{@code this.subscriptionPlugin != null && this.subscriptionPlugin.isInitialised()}
 * invariant	the WindTurbine has exactly 1 published inbound port implementing ReceivingCI. // informal
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class WindTurbine extends Client {

	/** The component position. */
	protected final PositionData windTurbinePos;
	
	/** The test scenario followed by the component, if there is one. */
	protected final TestScenario testScenario;
	
	/** 
	 * Indicates if the wind turbine must subscribe to default channels 'channel0' and 'channel1' on execute().<br>
	 * True for non-distributed test scenarios, false for distributed scenarios because otherwise we could have
	 * subscription gossip messages propagating faster than registration gossip messages during setup, leading to errors.
	 */
	protected boolean subscriptionOnSetup;
	
	/** 
	 * The component registers itself on the "channel0" during execute() with this filter, if {@code subscriptionOnSetup == true}. 
	 * Otherwise stays null.
	 */
	protected MessageFilterI filterChannel0;
	
	/** 
	 * The component registers itself on the "channel1" during execute() with this filter, if {@code subscriptionOnSetup == true}. 
	 * Otherwise stays null.
	 */
	protected MessageFilterI filterChannel1;
	
	// Internal datas
	
	/** 
	 * The orientation of the wind turbine. 
	 * PositionData is used here as a vector, for the WindTurbine orientation. 
	 */
	protected PositionData orientation = new PositionData(0, 0);
	
	/** 
	 * Maximum alert level (inclusive) at which the turbine operates.<br>
	 * If a higher level is received, the turbine enters safety mode.
	 */
	protected final LevelI maxWorkingLevel;
	
	/** Indicates whether the turbine is in safety mode or not (false by default). */
	protected boolean safeMode = false;
	
	/** Stores wind data received from messages. */
	protected final ArrayList<WindData> datas = new ArrayList<WindData>();
	
	/** {@code safeMode} lock, used when the safemode is accessed for reading/writing. */
	protected final Object safeModeLock = new Object();
	
	// -------------------------------------------------------------------------
	// WindTurbine reception plug-in setup method
	// -------------------------------------------------------------------------
	
	/**
	 * Setup the {@code subscriptionPlugin} plug-in with the wind turbine version ({@link WindTurbineReceptionPlugin}).
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code this.registrationPlugin.isInitialised()}
	 * post	{@code this.subscriptionPlugin != null && this.subscriptionPlugin.isInitialised()}
	 * post	the WindTurbine has exactly 1 published inbound port implementing ReceivingCI. // informal
	 * </pre>
	 * 
	 * @throws Exception 	If an error occurs while setuping the plug-in.
	 */
	protected void setupWindTurbineSubscriptionPlugin() throws Exception {
		// Precondition check
		assert registrationPlugin.isInitialised() : // if initialized, registrationPlugin.getRegistrationOutboundPort() != null
			new PreconditionException("Registration plugin must be initialized.");
		
		// Install the subscription + reception plug-in
		this.subscriptionPlugin = new WindTurbineReceptionPlugin(registrationPlugin);
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

	//--------------------------------------------------------------------------
	// Constructors
	//--------------------------------------------------------------------------
	
	/**
     * Creates an active test wind turbine component for the publish/subscribe system.
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
	 * pre	{@code reflectionInboundPortURI != null && !reflectionInboundPortURI.isEmpty()}
     * pre  {@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
     * pre	{@code rc != null}
	 * pre	{@code nbThreads >= 0 && nbSchedulableThreads >= 0}
     * pre  {@code windTurbinePos != null}
     * pre  {@code maxWorkingLevel != null}
	 * pre  {@code filterChannel0 != null && filterChannel01!= null}
	 * pre  {@code testScenario != null}
     * post {@code true} // no postcondition.
     * </pre>
     * 
	 * @param id						  	A number identifying the component, just for tracer window.
	 * @param reflectionInboundPortURI	  	URI of the reflection inbound port to be created in this component.
     * @param receivingInboundPortURI     	URI of the inbound port providing {@link ReceivingCI}.
	 * @param rc			  		 	  	The registration class of the component.
	 * @param nbThreads					  	Number of threads to be created in the component pool.
	 * @param nbSchedulableThreads		  	Number of threads to be created in the component schedulable pool.
     * @param windTurbinePos				The position of the wind turbine.
     * @param maxWorkingLevel             	Maximum alert level (inclusive) at which the turbine operates.
	 * @param subscriptionOnSetup			Indicates if the wind turbine must subscribe to default channels 'channel0' and 'channel1' on execute.
	 * @param filterChannel0			  	Filter used to register during {@code execute} method on the "channel0".
	 * @param filterChannel1			  	Filter used to register during {@code execute} method on the "channel1".
	 * @param testScenario				  	Test scenario in which this component participate.
     * @throws Exception                  	If a problem occurs during initialization or port publication.
     */
	protected WindTurbine(int id, String reflectionInboundPortURI, String receivingInboundPortURI, RegistrationClass rc, 
			int nbThreads, int nbSchedulableThreads, PositionData windTurbinePos, LevelI maxWorkingLevel, 
			boolean subscriptionOnSetup, MessageFilterI filterChannel0, MessageFilterI filterChannel1, TestScenario testScenario) 
					throws Exception {
		super(reflectionInboundPortURI, receivingInboundPortURI, rc, nbThreads, nbSchedulableThreads);
		
		// Some preconditions are checked inside the super-constructor while others are checked here
		assert (windTurbinePos != null) : 
			new PreconditionException("WindTurbine position cannot be null");	
		assert (maxWorkingLevel != null) : 
			new PreconditionException("max working level cannot be null");	
		assert (filterChannel0 != null) : 
			new PreconditionException("filterChannel0 cannot be null");	
		assert (filterChannel1 != null) : 
			new PreconditionException("filterChannel1 cannot be null");	
		assert (testScenario != null) : 
			new PreconditionException("testScenario cannot be null");	
		
		this.windTurbinePos = windTurbinePos;
		this.maxWorkingLevel = maxWorkingLevel;
		this.testScenario = testScenario;
		this.subscriptionOnSetup = subscriptionOnSetup;
		this.filterChannel0 = filterChannel0;
		this.filterChannel1 = filterChannel1;
		
		// Install the subscription + reception plugin (Wind turbine version)
		setupWindTurbineSubscriptionPlugin();
		
		this.getTracer().setTitle("WindTurbine "+id);
		this.getTracer().setRelativePosition((int)windTurbinePos.getPosX(), (int)windTurbinePos.getPosY());
	}
	
	/**
     * Creates a passive wind turbine component for the publish/subscribe system.
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
	 * pre	{@code reflectionInboundPortURI != null && !reflectionInboundPortURI.isEmpty()}
     * pre  {@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
     * pre	{@code rc != null}
	 * pre	{@code nbThreads >= 0 && nbSchedulableThreads >= 0}
     * pre  {@code windTurbinePos != null}
     * pre  {@code maxWorkingLevel != null}
	 * pre  {@code filterChannel0 != null && filterChannel01!= null}
     * post {@code true} // no postcondition.
     * </pre>
     * 
	 * @param id						  	A number identifying the component, just for tracer window.
	 * @param reflectionInboundPortURI	  	URI of the reflection inbound port to be created in this component.
     * @param receivingInboundPortURI     	URI of the inbound port providing {@link ReceivingCI}.
	 * @param rc			  		 	  	The registration class of the component.
	 * @param nbThreads					  	Number of threads to be created in the component pool.
	 * @param nbSchedulableThreads		  	Number of threads to be created in the component schedulable pool.
     * @param windTurbinePos 				The position of the wind turbine.
     * @param maxWorkingLevel             	Maximum alert level (inclusive) at which the turbine operates.
	 * @param subscriptionOnSetup			Indicates if the wind turbine must subscribe to default channels 'channel0' and 'channel1' on execute.
	 * @param filterChannel0			  	Filter used to register during {@code execute} method on the "channel0".
	 * @param filterChannel1			  	Filter used to register during {@code execute} method on the "channel1".
     * @throws Exception                  	If a problem occurs during initialization or port publication.
     */
	protected WindTurbine(int id, String reflectionInboundPortURI, String receivingInboundPortURI, RegistrationClass rc, 
			int nbThreads, int nbSchedulableThreads, PositionData windTurbinePos, LevelI maxWorkingLevel, 
			boolean subscriptionOnSetup, MessageFilterI filterChannel0, MessageFilterI filterChannel1) throws Exception {
		super(reflectionInboundPortURI, receivingInboundPortURI, rc, nbThreads, nbSchedulableThreads);
		
		// Some preconditions are checked inside the super-constructor while others are checked here
		assert (windTurbinePos != null) : 
			new PreconditionException("WindTurbine position cannot be null");	
		assert (maxWorkingLevel != null) : 
			new PreconditionException("max working level cannot be null");	
		assert (filterChannel0 != null) : 
			new PreconditionException("filterChannel0 cannot be null");	
		assert (filterChannel1 != null) : 
			new PreconditionException("filterChannel1 cannot be null");	
		
		this.windTurbinePos = windTurbinePos;
		this.maxWorkingLevel = maxWorkingLevel;
		this.testScenario = null;
		this.subscriptionOnSetup = subscriptionOnSetup;
		this.filterChannel0 = filterChannel0;
		this.filterChannel1 = filterChannel1;
		
		// Install the subscription + reception plugin (Wind turbine version)
		setupWindTurbineSubscriptionPlugin();
		
		this.getTracer().setTitle("WindTurbine");
		this.getTracer().setRelativePosition((int)windTurbinePos.getPosX(), (int)windTurbinePos.getPosY());
	}

	// --------------------------------------------------------------------------
	// Component life-cycle
	// --------------------------------------------------------------------------

	@Override
	public void execute() throws Exception {
		super.execute();
		this.logMessage("executing WindTurbine component");

		// Register on the Broker
		registrationPlugin.register(registrationClass);
		assert (registrationPlugin.getBrokerPublishingPortURI() != null);
		this.logMessage("WindTurbine registered");
		
		if (subscriptionOnSetup) {
	        // Wind data subscription
	        subscriptionPlugin.subscribe("channel0", filterChannel0);
	        this.logMessage("Thread "+Thread.currentThread().getId()+": WindTurbine subscribed to channel0");

	        // Alert data subscription
	        subscriptionPlugin.subscribe("channel1", filterChannel1);
	        this.logMessage("Thread "+Thread.currentThread().getId()+": WindTurbine subscribed to channel1");
		}
        
		// Install the privileged plug-in if it is allowed.
		if (this.registrationClass == RegistrationClass.STANDARD || this.registrationClass == RegistrationClass.PREMIUM) {
			setupPrivilegedPlugin();
		}

		if (testScenario != null) {
			// Initialization of the clock
			this.initialiseClock(
					ClocksServer.STANDARD_INBOUNDPORT_URI,
					this.testScenario.getClockURI());
			// Component ready for executing the scenario
			this.executeTestScenario(this.testScenario);
		}
	}
	
	// --------------------------------------------------------------------------
	// Component public methods
	// --------------------------------------------------------------------------
	
	/**
	 * Indicates if the wind turbine is in safe mode.
	 * 
	 * <p><strong>Contrat</strong></p>
	 * <pre>
	 * pre  {@code true}	// no precondition.
	 * post	{@code true} 	// no postcondition.
	 * </pre>
	 * 
	 * @return true		if wind turbine is in safe mode, otherwise it operates normally.
	 */
	public boolean inSafeMode() {
		synchronized(safeModeLock) {
			return safeMode;
		}
	}

    /**
     * Calculates the orientation to adopt according to the collected wind data so far + the {@code newWindData} added.
     * 
	 * <p><strong>Contrat</strong></p>
	 * <pre>
	 * pre  {@code true}	// no precondition.
	 * post	{@code datas.contains(newWindData)}
	 * </pre>
	 * 
	 * @param newWindData New wind data to consider.
     */
	public void considerNewWindData(WindData newWindData) {
		synchronized (this.datas) {
			if (newWindData != null) {
				datas.add(newWindData);
			}
			
		    double x = 0;
		    double y = 0;
		    double sumWeights = 0;

		    for (WindData d : datas) {
		        double distance = distance((PositionData) d.getPosition()); // having a WindData implies that its position is a 'PositionData'

		        double weight;
		        if (distance == 0) {
		            weight = 1.0;
		        } else {
		            weight = 1.0 / distance;
		        }

		        x += d.xComponent() * weight;
		        y += d.yComponent() * weight;

		        sumWeights += weight;
		    }

		    if (sumWeights != 0) {
		        x = x / sumWeights;
		        y = y / sumWeights;
		    }

		    // Reverse vector direction
		    x = -x;
		    y = -y;

			this.orientation = new PositionData(x, y);
			this.logMessage("Thread "+Thread.currentThread().getId()+": WindTurbine has changed orientation : now = " + this.orientation);
			
			// Postcondition check
			assert(datas.contains(newWindData)) : new PostconditionException("New wind data must be considered");	
		}	
	}
	
    /**
     * Reacts to the received alert level according to operation threshold.
     * 
	 * <p><strong>Contrat</strong></p>
	 * <pre>
	 * pre  {@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
     * @param alert Received alert.
     */
	public void considerNewAlert(MeteoAlert alert) {
		if (alert == null) {
			return;
		}
		
		MeteoAlertI.Level maxLevel = (MeteoAlertI.Level) this.maxWorkingLevel;
		MeteoAlertI.Level alertLevel = (MeteoAlertI.Level) alert.getLevel();

		synchronized(safeModeLock) {
			if (alertLevel.ordinal() > maxLevel.ordinal() && !safeMode) {
				goInSafeMode();
				synchronized (this.datas) {
					this.datas.clear(); // Erase previous wind datas
				}
			} else if (alertLevel.ordinal() <= maxLevel.ordinal() && safeMode) {
				goBacktoNormalMode();
			} else {
				// Don't change anything, if we have :
				// - received an alert above the maximum operating level while already in safe mode
				// - received an alert within the operating range while already in normal operation (safeMode == false)
			}
		}
	}
	
	// --------------------------------------------------------------------------
	// Component internal methods
	// --------------------------------------------------------------------------

    /**
     * Computes the distance between the turbine and a meteorological station.
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code stationPos != null}
     * post {@code true} // no postcondition.
     * </pre>
     * 
     * @param stationPos	Position of the meteorological station.
     * @return				the distance between the turbine and a {@code stationPos}.
     */
	protected double distance(PositionData stationPos) {
		// Precondition check
		assert(stationPos != null) : new PreconditionException("stationPos cannot be null");	
		
		double dx = stationPos.getPosX() - this.windTurbinePos.getPosX();
		double dy = stationPos.getPosY() - this.windTurbinePos.getPosY();
		return Math.sqrt(dx * dx + dy * dy);
	}
	
	/**
	 * WindTurbine enters safe mode, stops operating.
	 * 
	 * <p><strong>Contrat</strong></p>
	 * <pre>
	 * pre {@code true}	// no precondition.
	 * post {@code this.safemode == true}
	 * </pre>
	 */
	protected void goInSafeMode() {
		synchronized(safeModeLock) {
			if (!safeMode) {
				this.logMessage("Thread "+Thread.currentThread().getId()+": WindTurbine going into security mode.");
				safeMode = true;
			}
			// Postcondition check
			assert(safeMode) : new PostconditionException("Safe mode must be turned on");
		}
	}

	/**
	 * WindTurbine returns to normal operation.
	 * 
	 * <p><strong>Contrat</strong></p>
	 * <pre>
	 * pre {@code true}	// no precondition.
	 * post {@code this.safemode == false}
	 * </pre>
	 */
	protected void goBacktoNormalMode() {
		synchronized(safeModeLock) {
			if (safeMode) {
				this.logMessage("Thread "+Thread.currentThread().getId()+": WindTurbine returning to normal operation.");
				safeMode = false;
			}
			// Postcondition check
			assert(!safeMode) : new PostconditionException("Safe mode must be turned off");
		}
	}
	
	// --------------------------------------------------------------------------
	// Test actions
	// --------------------------------------------------------------------------

	/**
	 * Unregisters the Wind Turbine from the system.
	 * 
	 * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code true} // no precondition.
     * post {@code !registrationPlugin.registered()}
     * </pre>
     * 
     * @throws Exception If an error occurs while unregistering.
	 */
	public void unregisterFromSystem()  throws Exception  {
		registrationPlugin.unregister();
		this.logMessage("WindTurbine unregistered from system");
		
		// Postcondition check
		assert(!registrationPlugin.registered()) : new PostconditionException("should be unregistered from system");
	}
	
	/**
	 * Unsubscribes from the {@code channel}.
	 * 
	 * <p><strong>Contract</strong></p>
     * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
     * post {@code !subscriptionPlugin.subscribed(channel)}
     * </pre>
	 * 
	 * @param channel		The channel from which the turbine must unsubscribe.
	 * @throws Exception	If an error occurs while unsubscribing.
	 */
	public void unsubscribeFromChannel(String channel)  throws Exception  {
		// Precondition check
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");
		
		subscriptionPlugin.unsubscribe(channel);
		this.logMessage("WindTurbine unsubscribed from channel : "+channel);
		
		// Postcondition check
		assert(!subscriptionPlugin.subscribed(channel)) : new PostconditionException("should be unsubscribed from channel : "+channel);
	}
	
	/**
	 * Subscribes to the {@code channel} with the given {@code filter}.
	 * 
	 * <p><strong>Contract</strong></p>
     * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code filter != null}
     * post {@code subscriptionPlugin.subscribed(channel)}
     * </pre>
	 * 
	 * @param channel		The channel to subscribe to.
	 * @param filter		The filter to apply on the {@code channel}
	 * @throws Exception	If an error occurs while subscribing.
	 */
	public void subscribeToChannel(String channel, MessageFilterI filter)  throws Exception  {
		// Precondition check
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");
		assert(filter != null) : new PreconditionException("filter cannot be null");
		
		subscriptionPlugin.subscribe(channel, filter);
		this.logMessage("WindTurbine subscribed to channel : "+channel);
		
		// Postcondition check
		assert(subscriptionPlugin.subscribed(channel)) : new PostconditionException("should be subscribed to channel : "+channel);
	}
	
	/**
	 * Verifies that the wind turbine has received {@code expectedNb} wind datas since its start.
	 * 
	 * <p><strong>Contract</strong></p>
     * <pre>
	 * pre	{@code true} // no precondition
     * post {@code true} // no postcondition
     * </pre>
	 * 
	 * @param expectedNb	The expected number of wind datas received on the wind turbine.
	 */
	public void verifyNbWindDatasReceived(int expectedNb) {
		int nbReceived;
		synchronized (this.datas) {
			nbReceived = this.datas.size();
		}
		assert(nbReceived == expectedNb);
		this.logMessage("WindTurbine expected "+expectedNb+" wind datas and got them : correct");
	}
	
	/**
	 * Gets the number of wind datas the wind turbine has received since its start.
	 * 
	 * <p><strong>Contract</strong></p>
     * <pre>
	 * pre	{@code true} // no precondition
     * post {@code true} // no postcondition
     * </pre>
     * 
	 * @return		The number of wind datas the wind turbine has received since its start.
	 */
	public int getNbWindDatasReceived() {
		synchronized (this.datas) {
			return this.datas.size();
		}
	}
	
	/**
	 * Waits a message from {@code channel}.
	 * 
	 * <p><strong>Contract</strong></p>
     * <pre>
	 * pre	{@code true} // no precondition
     * post {@code true} // no postcondition
     * </pre>
     * 
	 * @param channel		The channel on which the wait is done.
	 */
	public void waitMessage(String channel) {
		try {
			MessageI mes = subscriptionPlugin.waitForNextMessage(channel);
			System.out.println("waitForNextMessage ended, got message ? :"+(mes != null));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Waits a message from {@code channel}, for a given timeout of duration {@code d}.
	 * 
	 * <p><strong>Contract</strong></p>
     * <pre>
	 * pre	{@code true} // no precondition
     * post {@code true} // no postcondition
     * </pre>
     * 
	 * @param channel		The channel on which the wait is done.
	 * @param d				The timeout for the wait.
	 */
	public void waitMessageTimeout(String channel, Duration d) {
		try {
			MessageI mes = subscriptionPlugin.waitForNextMessage(channel, d);
			System.out.println("waitForNextMessage (timeout) ended, got message ? :"+(mes != null));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Gets the future for getting a message from {@code channel}, 
	 * sleeps during a {@code d} duration before getting the future content.
	 * 
	 * <p><strong>Contract</strong></p>
     * <pre>
	 * pre	{@code true} // no precondition
     * post {@code true} // no postcondition
     * </pre>
     * 
	 * @param channel					The channel for which the future is got.
	 * @param d							The thread sleep time, between the future getting and the {@code get} on the future.
	 */
	public void waitMessageFuture(String channel, Duration d) {
		try {
			Future<MessageI> future = subscriptionPlugin.getNextMessage(channel);
			Thread.sleep(d.toMillis());
			MessageI mes = future.get();
			System.out.println("getNextMessage (future) ended, got message ? :"+(mes != null));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
