package fr.sorbonne_u.meteo.tests;

import java.time.Duration;

import fr.sorbonne_u.components.utils.tests.TestScenario;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI.LevelI;
import fr.sorbonne_u.meteo.components.WindTurbine;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.utils.aclocks.ClocksServer;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.exceptions.PostconditionException;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * Test class representing a {@link WindTurbine}, following a predefined scenario.
 * 
 * <p>The class does extend {@link WindTurbine} because instead of only listening by default, here it has also defined actions to make.</p>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class WindTurbineTest extends WindTurbine {
	
	/** The test scenario followed by the component (can be null if the windturbine is not actively part of a scenario) */
	protected final TestScenario testScenario;
	
	/** If not null, the component registers itself on the "channel0" during execute() with this filter */
	protected MessageFilterI filterChannel0;
	
	/** If not null, the component registers itself on the "channel1" during execute() with this filter */
	protected MessageFilterI filterChannel1;

	//--------------------------------------------------------------------------
	// Constructor
	//--------------------------------------------------------------------------
	
	/**
     * Creates an active test wind turbine component for the publish/subscribe system.
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
     * pre  {@code registrationOutboundPortURI != null && !registrationOutboundPortURI.isEmpty()}
     * pre  {@code publishingOutboundPortURI != null && !publishingOutboundPortURI.isEmpty()}
     * pre	{@code rc != null}
	 * pre	{@code nbThreads >= 0 && nbSchedulableThreads >= 0}
     * pre  {@code pos != null}
     * pre  {@code maxWorkingLevel != null}
	 * pre	{@code reflectionInboundPortURI != null && !reflectionInboundPortURI.isEmpty()}
	 * pre  {@code testScenario != null}
	 * pre  {@code filterChannel0 != null && filterChannel01!= null}
     * post {@code true} // no postcondition.
     * </pre>
     * 
     * @param receivingInboundPortURI     URI of the inbound port providing {@link ReceivingCI}.
     * @param registrationOutboundPortURI URI of the outbound port requesting {@link RegistrationCI}.
     * @param publishingOutboundPortURI   URI of the outbound port requesting {@link PublishingCI}.
	 * @param rc			  		 	  The registration class of the component.
	 * @param nbThreads					  Number of threads to be created in the component pool.
	 * @param nbSchedulableThreads		  Number of threads to be created in the component schedulable pool.
     * @param pos                         The position of the wind turbine.
     * @param maxWorkingLevel             Maximum alert level (inclusive) at which the turbine operates.
	 * @param reflectionInboundPortURI	  URI of the reflection inbound port to be created in this component.
	 * @param testScenario				  Test scenario in which this component participate.
	 * @param id						  A number identifying the component, just for tracer window.
	 * @param filterChannel0			  Filter used to register during {@code execute} method on the "channel0".
	 * @param filterChannel1			  Filter used to register during {@code execute} method on the "channel1".
     * @throws Exception                  If a problem occurs during initialization or port publication.
     */
	protected WindTurbineTest(String receivingInboundPortURI, String registrationOutboundPortURI, String publishingOutboundPortURI, 
			RegistrationClass rc, int nbThreads, int nbSchedulableThreads, PositionData pos,LevelI maxWorkingLevel, 
			String reflectionInboundPortURI, TestScenario testScenario, int id, MessageFilterI filterChannel0, MessageFilterI filterChannel1) throws Exception {
		super(receivingInboundPortURI, registrationOutboundPortURI, publishingOutboundPortURI, rc, nbThreads, nbSchedulableThreads, pos, maxWorkingLevel, reflectionInboundPortURI);
		
		// Preconditions are checked inside the superconstructor
		
		this.testScenario = testScenario;
		this.getTracer().setTitle("WindTurbine "+id);
		this.defaultReceptionOnExec = false; // Default reception on execute() method disabled
		this.filterChannel0 = filterChannel0;
		this.filterChannel1 = filterChannel1;
	}
	
	/**
     * Creates a passive test wind turbine component for the publish/subscribe system.
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
     * pre  {@code registrationOutboundPortURI != null && !registrationOutboundPortURI.isEmpty()}
     * pre  {@code publishingOutboundPortURI != null && !publishingOutboundPortURI.isEmpty()}
     * pre	{@code rc != null}
	 * pre	{@code nbThreads >= 0 && nbSchedulableThreads >= 0}
     * pre  {@code pos != null}
     * pre  {@code maxWorkingLevel != null}
	 * pre	{@code reflectionInboundPortURI != null && !reflectionInboundPortURI.isEmpty()}
	 * pre  {@code filterChannel0 != null && filterChannel01!= null}
     * post {@code true} // no postcondition.
     * </pre>
     * 
     * @param receivingInboundPortURI     URI of the inbound port providing {@link ReceivingCI}.
     * @param registrationOutboundPortURI URI of the outbound port requesting {@link RegistrationCI}.
     * @param publishingOutboundPortURI   URI of the outbound port requesting {@link PublishingCI}.
	 * @param rc			  		 	  The registration class of the component.
	 * @param nbThreads					  Number of threads to be created in the component pool.
	 * @param nbSchedulableThreads		  Number of threads to be created in the component schedulable pool.
     * @param pos                         The position of the wind turbine.
     * @param maxWorkingLevel             Maximum alert level (inclusive) at which the turbine operates.
	 * @param reflectionInboundPortURI	  URI of the reflection inbound port to be created in this component.
	 * @param id						  A number identifying the component, just for tracer window.
	 * @param filterChannel0			  Filter used to register during {@code execute} method on the "channel0".
	 * @param filterChannel1			  Filter used to register during {@code execute} method on the "channel1".
     * @throws Exception                  If a problem occurs during initialization or port publication.
     */
	protected WindTurbineTest(String receivingInboundPortURI, String registrationOutboundPortURI, String publishingOutboundPortURI, 
			RegistrationClass rc, int nbThreads, int nbSchedulableThreads, PositionData pos,LevelI maxWorkingLevel, 
			String reflectionInboundPortURI, int id, MessageFilterI filterChannel0, MessageFilterI filterChannel1) throws Exception {
		super(receivingInboundPortURI, registrationOutboundPortURI, publishingOutboundPortURI, rc, nbThreads, nbSchedulableThreads, pos, maxWorkingLevel, reflectionInboundPortURI);
		
		// Preconditions are checked inside the superconstructor
		
		this.testScenario = null;
		this.getTracer().setTitle("WindTurbine "+id);
		this.defaultReceptionOnExec = false; // Default reception on execute() method disabled
		this.filterChannel0 = filterChannel0;
		this.filterChannel1 = filterChannel1;
	}

	// --------------------------------------------------------------------------
	// Component life-cycle
	// --------------------------------------------------------------------------

	@Override
	public void execute() throws Exception {
		super.execute();
		
        // Wind data subscription
        subscriptionPlugin.subscribe("channel0", filterChannel0);
        this.logMessage("Thread "+Thread.currentThread().getId()+": WindTurbine subscribed to channel0");

        // Alert data subscription
        subscriptionPlugin.subscribe("channel1", filterChannel1);
        this.logMessage("Thread "+Thread.currentThread().getId()+": WindTurbine subscribed to channel1");
		
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
	public void unsubscibeFromChannel(String channel)  throws Exception  {
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
	public void subscibeToChannel(String channel, MessageFilterI filter)  throws Exception  {
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
	
	public void gettingNextMessage(String channel) {
		//System.out.println("gettingNextMessage");
		try {
			MessageI mes = subscriptionPlugin.waitForNextMessage(channel);
			System.out.println("waitForNextMessage ended, got message ? :"+(mes != null));
			this.logMessage("Client received message '" + mes + "' from the channel : " + channel);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void gettingNextMessageTime(String channel,Duration d) {
		//System.out.println("gettingNextMessageTime");
		try {
			MessageI mes = subscriptionPlugin.waitForNextMessage(channel, d);
			System.out.println("waitForNextMessage timeout ended, got message ? :"+(mes != null));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void gettingNextMessageFuture(String channel) {
		//System.out.println("gettingNextMessageFuture");
		try {
			MessageI mes = subscriptionPlugin.getNextMessage(channel).get();
			System.out.println("getNextMessage ended,  got message ? :"+(mes != null));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
