package fr.sorbonne_u.meteo.tests;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;

import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.clients.plugins.ClientPrivilegedPlugin;
import fr.sorbonne_u.clients.plugins.ClientPublicationPlugin;
import fr.sorbonne_u.clients.plugins.ClientSubscriptionPlugin;
import fr.sorbonne_u.components.utils.tests.TestScenario;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI;
import fr.sorbonne_u.cps.meteo.interfaces.RegionI;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.messages.Message;
import fr.sorbonne_u.meteo.data.MeteoAlert;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.utils.aclocks.ClocksServer;

/**
 * Test class representing a {@link MeteoBureau} following a predefined scenario.
 * 
 * <p>The scenario is described in the {@link CVMTest} class.</p>
 * 
 * <p>The class does not extend {@link MeteoBureau} because it has already a defined random behavior on launch, here it is not the case.</p>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class MeteoBureauTest extends Client{
	
	protected final PositionData bureauPosition;
	
	// Publication
	protected final String publishingOutboundPortURI; 
	protected ClientPublicationPlugin publicationPlugin;
	
	// Subscription + reception
	protected final ClientSubscriptionPlugin subscriptionPlugin;
	
	// Privileged management
	protected String privilegedOutboundPortURI = null; // stays null if the client is not privileged
	protected ClientPrivilegedPlugin privilegedPlugin = null; // stays null if the client is not privileged
	
	// Scenario
	protected final TestScenario testScenario;

	//---------------------------------------------------------------------------
	// Constructor
	//---------------------------------------------------------------------------
	
	/**
	 * Creates a meteorological office component for the publish/subscribe system, extending {@link Client}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code registrationOutboundPortURI != null && !registrationOutboundPortURI.isEmpty()}
	 * pre	{@code publishingOutboundPortURI != null && !publishingOutboundPortURI.isEmpty()}
	 * pre	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
	 * pre	{@code privilegedOutboundPortURI != null}, if {@code rc != FREE : !privilegedOutboundPortURI.isEmpty()}
	 * pre	{@code nbThreads >= 0 && nbSchedulableThreads >= 0}
	 * pre	{@code bureauPosition != null}
	 * pre  {@code testScenario != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param registrationOutboundPortURI   URI of the outbound port requiring {@link RegistrationCI}.
	 * @param publishingOutboundPortURI     URI of the outbound port requiring {@link PublishingCI}.
	 * @param receivingInboundPortURI       URI of the inbound port offering {@link ReceivingCI}.
	 * @param privilegedOutboundPortURI	    URI of the outbound port requiring {@link PrivilegedClientCI}. Can be empty if the client if FREE, otherwise it cannot.
	 * @param rc			   				The registration class of the component.
	 * @param bureauPosition                The position of the office.
	 * @param testScenario					Test scenario in which this component participate.
	 * @param reflectionInboundPortURI		URI of the reflection inbound port to be created in this component.
	 * @throws Exception                    If an error occurs during initialization or port publication.
	 */
	protected MeteoBureauTest(String registrationOutboundPortURI, String publishingOutboundPortURI, String receivingInboundPortURI, String privilegedOutboundPortURI, 
			RegistrationClass rc, int nbThreads, int nbSchedulableThreads, PositionData bureauPosition, TestScenario testScenario, String reflectionInboundPortURI) throws Exception {
		super(receivingInboundPortURI, registrationOutboundPortURI, rc, nbThreads, nbSchedulableThreads, reflectionInboundPortURI);
		// Some preconditions are checked inside the superconstructor while others are checked here
		assert(bureauPosition != null);
		assert(testScenario != null);
		assert(privilegedOutboundPortURI != null);
		if (rc != RegistrationClass.FREE) {
			assert (!privilegedOutboundPortURI.isEmpty());
			this.privilegedOutboundPortURI = privilegedOutboundPortURI;
		}
		
		this.bureauPosition = bureauPosition;
		this.publishingOutboundPortURI = publishingOutboundPortURI;
		this.testScenario = testScenario;
		
		// Install the subscription + reception plugin
		this.subscriptionPlugin = new ClientSubscriptionPlugin(receivingInboundPortURI, registrationPlugin.getRegistrationOutboundPort());
		this.subscriptionPlugin.setPluginURI("subscription");
		this.installPlugin(this.subscriptionPlugin);
		
		this.getTracer().setTitle("MeteoBureau") ;
		this.getTracer().setRelativePosition((int)bureauPosition.getPosX(), (int)bureauPosition.getPosY()) ;
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
		
		// Install the privileged plug-in if it is allowed.
		if (this.registrationClass == RegistrationClass.STANDARD || this.registrationClass == RegistrationClass.PREMIUM) {
			this.privilegedPlugin = new ClientPrivilegedPlugin(receivingInboundPortURI, privilegedOutboundPortURI, registrationPlugin.getBrokerPublishingPortURI());
			this.privilegedPlugin.setPluginURI("privileged");
			this.installPlugin(this.privilegedPlugin);
		}
		
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
		assert(type != null);
		assert(level != null);
		assert(regions != null);
		assert(instant != null);
		assert(duration != null);
		assert(channel != null && !channel.isEmpty());
		
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