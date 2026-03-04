package fr.sorbonne_u.meteo.tests;

import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.clients.plugins.ClientPrivilegedPlugin;
import fr.sorbonne_u.clients.plugins.ClientPublicationPlugin;
import fr.sorbonne_u.clients.plugins.ClientSubscriptionPlugin;
import fr.sorbonne_u.components.utils.tests.TestScenario;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.messages.Message;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.meteo.data.WindData;
import fr.sorbonne_u.utils.aclocks.ClocksServer;

/**
 * Class representing a {@link MeteoStation} emitting wind data following a predefined scenario.
 * 
 * <p>The scenario is described in the {@link CVMTest} class.</p>
 * 
 * <p>The class does not extend {@link MeteoStation} because it has already a defined random behavior on launch, here it is not the case.</p>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class MeteoStationTest extends Client {
	
	protected final PositionData stationPosition;
	
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
	
	// --------------------------------------------------------------------------
	// Constructor
	// --------------------------------------------------------------------------

	/**
	 * Creates a meteorological station component of the publish/subscribe system, extending {@link Client}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code registrationOutboundPortURI != null && !registrationOutboundPortURI.isEmpty()}
	 * pre	{@code publishingOutboundPortURI != null && !publishingOutboundPortURI.isEmpty()}
	 * pre	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
	 * pre	{@code privilegedOutboundPortURI != null}, if {@code rc != FREE : !privilegedOutboundPortURI.isEmpty()}
	 * pre	{@code nbThreads >= 0 && nbSchedulableThreads >= 0}
	 * pre	{@code stationPosition != null}
	 * pre  {@code testScenario != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param registrationOutboundPortURI   URI of the outbound port requiring {@link RegistrationCI}.
	 * @param publishingOutboundPortURI     URI of the outbound port requiring {@link PublishingCI}.
	 * @param receivingInboundPortURI       URI of the inbound port offering {@link ReceivingCI}.
	 * @param privilegedOutboundPortURI	    URI of the outbound port requiring {@link PrivilegedClientCI}. Can be empty if the client if FREE, otherwise it cannot.
	 * @param rc			   				The registration class of the component.
	 * @param stationPosition               The position of the station.
	 * @param reflectionInboundPortURI		URI of the reflection inbound port to be created in this component.
	 * @throws Exception                    If an error occurs during initialization or port publication.
	 */
	protected MeteoStationTest(String registrationOutboundPortURI, String publishingOutboundPortURI, String receivingInboundPortURI, String privilegedOutboundPortURI, 
			RegistrationClass rc, int nbThreads, int nbSchedulableThreads, PositionData stationPosition, TestScenario testScenario, String reflectionInboundPortURI) throws Exception {
		super(receivingInboundPortURI, registrationOutboundPortURI, rc, nbThreads, nbSchedulableThreads, reflectionInboundPortURI);
		// Some preconditions are checked inside the superconstructor while others are checked here
		assert(stationPosition != null);
		assert(testScenario != null);
		if (rc != RegistrationClass.FREE) {
			assert (!privilegedOutboundPortURI.isEmpty());
			this.privilegedOutboundPortURI = privilegedOutboundPortURI;
		}
		
		this.stationPosition = stationPosition;
		this.publishingOutboundPortURI = publishingOutboundPortURI;
		this.testScenario = testScenario;
		
		// Install the subscription + reception plugin
		this.subscriptionPlugin = new ClientSubscriptionPlugin(receivingInboundPortURI, registrationPlugin.getRegistrationOutboundPort());
		this.subscriptionPlugin.setPluginURI("subscription");
		this.installPlugin(this.subscriptionPlugin);
		
		this.getTracer().setTitle("MeteoStation");
		this.getTracer().setRelativePosition((int)stationPosition.getPosX(), (int)stationPosition.getPosY()) ;
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
		assert(channel != null && !channel.isEmpty());
		
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