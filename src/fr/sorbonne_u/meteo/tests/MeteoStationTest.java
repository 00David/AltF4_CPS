package fr.sorbonne_u.meteo.tests;

import fr.sorbonne_u.components.utils.tests.TestScenario;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.exceptions.PreconditionException;
import fr.sorbonne_u.messages.Message;
import fr.sorbonne_u.meteo.components.MeteoStation;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.meteo.data.WindData;
import fr.sorbonne_u.utils.aclocks.ClocksServer;

/**
 * Class representing a {@link MeteoStation} emitting wind data following a predefined scenario.
 * 
 * <p>The scenario is described in the {@link CVMTest} class.</p>
 * 
 * <p>The class does extend {@link MeteoStation} because it keeps the same setup, while adding personalized actions for a test scenario.</p>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class MeteoStationTest extends MeteoStation {

	/** The test scenario followed by the component */
	protected final TestScenario testScenario;
	
	// --------------------------------------------------------------------------
	// Constructor
	// --------------------------------------------------------------------------

	/**
	 * Creates a test meteorological station component of the publish/subscribe system, extending {@link MeteoStation}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
	 * pre	{@code registrationOutboundPortURI != null && !registrationOutboundPortURI.isEmpty()}
	 * pre	{@code publishingOutboundPortURI != null && !publishingOutboundPortURI.isEmpty()}
	 * pre	{@code privilegedOutboundPortURI != null}, if {@code rc != FREE : !privilegedOutboundPortURI.isEmpty()}
	 * pre	{@code nbThreads >= 0 && nbSchedulableThreads >= 0}
	 * pre	{@code stationPosition != null}
	 * pre	{@code reflectionInboundPortURI != null && !reflectionInboundPortURI.isEmpty()}
	 * pre  {@code testScenario != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param receivingInboundPortURI       URI of the inbound port offering {@link ReceivingCI}.
	 * @param registrationOutboundPortURI   URI of the outbound port requiring {@link RegistrationCI}.
	 * @param publishingOutboundPortURI     URI of the outbound port requiring {@link PublishingCI}.
	 * @param privilegedOutboundPortURI	    URI of the outbound port requiring {@link PrivilegedClientCI}. Can be empty if the client if FREE, otherwise it cannot.
	 * @param rc			   				The registration class of the component.
	 * @param stationPosition               The position of the station.
	 * @param reflectionInboundPortURI		URI of the reflection inbound port to be created in this component.
	 * @throws Exception                    If an error occurs during initialization or port publication.
	 */
	protected MeteoStationTest(String receivingInboundPortURI, String registrationOutboundPortURI, String publishingOutboundPortURI, String privilegedOutboundPortURI, 
			RegistrationClass rc, int nbThreads, int nbSchedulableThreads, PositionData stationPosition, String reflectionInboundPortURI, TestScenario testScenario) throws Exception {
		super(receivingInboundPortURI, registrationOutboundPortURI , publishingOutboundPortURI, privilegedOutboundPortURI, rc, nbThreads,
				nbSchedulableThreads, stationPosition, reflectionInboundPortURI);
		
		// Most preconditions are checked inside the superconstructor while the last one is checked here
		assert(testScenario != null) : new PreconditionException("testScenario cannot be null");	
		this.testScenario = testScenario;
	}
	
	// --------------------------------------------------------------------------
	// Component life-cycle
	// --------------------------------------------------------------------------

	@Override
	public void execute() throws Exception {
		super.execute();

		// Initialization of the clock
		this.initialiseClock(
				ClocksServer.STANDARD_INBOUNDPORT_URI,
				this.testScenario.getClockURI());
		// Component ready for executing the scenario
		this.executeTestScenario(this.testScenario);
	}
	
	// --------------------------------------------------------------------------
	// Sending WindDatas
	// --------------------------------------------------------------------------

	/**
	 * Publishes a {@link WindData} with given parameters on the {@code channel}, from the position of the MeteoStation.
	 * 
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param xWind 		The X component of the wind vector.
	 * @param yWind			The Y component of the wind vector.
	 * @param channel		The channel on which it must publish the wind data.
	 * @throws Exception	If an error occurs while publishing.
	 */
	public void publishWindData(double xWind, double yWind, String channel) throws Exception {
		// Precondition checks
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");	
		
		Message msg = new Message();
		WindData wd = new WindData(stationPosition, xWind, yWind);
		msg.setPayload(wd);
		msg.putProperty("position", stationPosition);
		msg.putProperty("xWind", xWind);
		msg.putProperty("yWind", yWind);
			
		this.logMessage("------------------------------");
		this.logMessage("MeteoStation sending wind data "+wd+"\nto channel : '"+channel+"'");
		this.logMessage("------------------------------");
		
		publicationPlugin.publish(channel, msg);
	}
}