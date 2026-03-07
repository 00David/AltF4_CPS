package fr.sorbonne_u.meteo.tests;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;

import fr.sorbonne_u.components.utils.tests.TestScenario;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI;
import fr.sorbonne_u.cps.meteo.interfaces.RegionI;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.exceptions.PreconditionException;
import fr.sorbonne_u.messages.Message;
import fr.sorbonne_u.meteo.components.MeteoBureau;
import fr.sorbonne_u.meteo.data.MeteoAlert;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.utils.aclocks.ClocksServer;

/**
 * Test class representing a {@link MeteoBureau} following a predefined scenario.
 * 
 * <p>The scenario is described in the {@link CVMTest} class.</p>
 * 
 * <p>The class does extend {@link MeteoBureau} because it keeps the same setup, while adding personalized actions for a test scenario.</p>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class MeteoBureauTest extends MeteoBureau {
	
	/** The test scenario followed by the component */
	protected final TestScenario testScenario;

	//---------------------------------------------------------------------------
	// Constructor
	//---------------------------------------------------------------------------
	
	/**
	 * Creates a test meteorological office component for the publish/subscribe system, extending {@link MeteoBureau}.
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
	 * pre  {@code testScenario != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param receivingInboundPortURI       URI of the inbound port offering {@link ReceivingCI}.
	 * @param registrationOutboundPortURI   URI of the outbound port requiring {@link RegistrationCI}.
	 * @param publishingOutboundPortURI     URI of the outbound port requiring {@link PublishingCI}.
	 * @param privilegedOutboundPortURI	    URI of the outbound port requiring {@link PrivilegedClientCI}. Can be empty if the client if FREE, otherwise it cannot.
	 * @param rc			   				The registration class of the component.
	 * @param bureauPosition                The position of the office.
	 * @param reflectionInboundPortURI		URI of the reflection inbound port to be created in this component.
	 * @param testScenario					Test scenario in which this component participate.
	 * @throws Exception                    If an error occurs during initialization or port publication.
	 */
	protected MeteoBureauTest(String receivingInboundPortURI, String registrationOutboundPortURI, String publishingOutboundPortURI, String privilegedOutboundPortURI, 
			RegistrationClass rc, int nbThreads, int nbSchedulableThreads, PositionData bureauPosition, String reflectionInboundPortURI, TestScenario testScenario) throws Exception {
		super(receivingInboundPortURI, registrationOutboundPortURI , publishingOutboundPortURI, privilegedOutboundPortURI, rc, nbThreads,
				nbSchedulableThreads, bureauPosition, reflectionInboundPortURI);
		
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
	// Sending alerts
	// --------------------------------------------------------------------------
	
	/**
	 * Publishes an alert on the given {@code channel}, with the given parameters.
	 * 
	 * <pre>
	 * pre	{@code type != null}
	 * pre	{@code level != null}
	 * pre	{@code regions != null}
	 * pre	{@code instant != null}
	 * pre	{@code duration != null}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param type			The alert type.
	 * @param level			The alert level.
	 * @param regions		The regions affected by the alert.
	 * @param instant		The start time of the alert.
	 * @param duration 		The duration of the alert.
	 * @param channel		The channel on which it must publish the alert.
	 * @throws Exception	If an error occurs while publishing.
	 */
	public void publishAlert(MeteoAlertI.AlertType type, MeteoAlertI.Level level, ArrayList<RegionI> regions, 
			Instant instant, Duration duration, String channel) throws Exception {
		// Precondition checks
		assert(type != null) : new PreconditionException("type cannot be null");
		assert(level != null) : new PreconditionException("level cannot be null");
		assert(regions != null) : new PreconditionException("regions cannot be null");
		assert(instant != null) : new PreconditionException("instant cannot be null");
		assert(duration != null) : new PreconditionException("duration cannot be null");
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");
		
		Message msg = new Message();
		MeteoAlert ma = new MeteoAlert(type, level, regions, instant, duration);
		msg.setPayload(ma);
		msg.putProperty("alert", type);
		msg.putProperty("level", level);
		msg.putProperty("regions", regions);
		msg.putProperty("instant", instant);
		msg.putProperty("duration", duration);
			
		this.logMessage("------------------------------");
		this.logMessage("MeteoBureau sending a "+level+" alert "+ma+"\nto channel : '"+channel+"'");
		this.logMessage("------------------------------");
		
		publicationPlugin.publish(channel, msg);
	}
}