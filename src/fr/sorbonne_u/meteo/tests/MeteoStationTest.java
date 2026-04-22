package fr.sorbonne_u.meteo.tests;

import java.util.ArrayList;
import java.util.List;

import fr.sorbonne_u.components.utils.tests.TestScenario;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.PrivilegedClientCI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.exceptions.PostconditionException;
import fr.sorbonne_u.exceptions.PreconditionException;
import fr.sorbonne_u.messages.Message;
import fr.sorbonne_u.messages.meteo.WindMessage;
import fr.sorbonne_u.meteo.components.MeteoStation;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.meteo.data.WindData;
import fr.sorbonne_u.utils.aclocks.ClocksServer;

/**
 * Class representing a {@link MeteoStation} emitting wind data following a predefined scenario.
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
	 * @param nbThreads					  	Number of threads to be created in the component pool.
	 * @param nbSchedulableThreads		  	Number of threads to be created in the component schedulable pool.
	 * @param stationPosition               The position of the station.
	 * @param reflectionInboundPortURI		URI of the reflection inbound port to be created in this component.
	 * @param testScenario				  	Test scenario in which this component participate.
	 * @param id							A number identifying the component, just for tracer window.
	 * @throws Exception                    If an error occurs during initialization or port publication.
	 */
	protected MeteoStationTest(String receivingInboundPortURI, String registrationOutboundPortURI, String publishingOutboundPortURI, String privilegedOutboundPortURI, 
			RegistrationClass rc, int nbThreads, int nbSchedulableThreads, PositionData stationPosition, String reflectionInboundPortURI, 
			TestScenario testScenario, int id) throws Exception {
		super(receivingInboundPortURI, registrationOutboundPortURI , publishingOutboundPortURI, privilegedOutboundPortURI, rc, nbThreads,
				nbSchedulableThreads, stationPosition, reflectionInboundPortURI);
		
		// Most preconditions are checked inside the superconstructor while the last one is checked here
		assert(testScenario != null) : new PreconditionException("testScenario cannot be null");	
		
		this.testScenario = testScenario;
		this.getTracer().setTitle("MeteoStation "+id);
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
	// Tests actions
	// --------------------------------------------------------------------------

	/**
	 * Publishes a {@link WindData} with given parameters on the {@code channel}, from the position of the MeteoStation.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param xWind 		The X component of the wind vector.
	 * @param yWind			The Y component of the wind vector.
	 * @param channel		The channel on which it must publish the wind data.
	 * @param notify		Indicates if the publishing service with notification must be called or not.
	 * @throws Exception	If an error occurs while publishing.
	 */
	public void publishWindData(double xWind, double yWind, String channel, boolean notify) throws Exception {
		// Precondition checks
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");	
		
		WindData wd = new WindData(stationPosition, xWind, yWind);
		Message msg = new WindMessage(wd);
			
		this.logMessage("------------------------------");
		this.logMessage("MeteoStation sending wind data "+wd+"\nto channel : '"+channel+"'");
		this.logMessage("------------------------------");
		
		if (notify) {
			publicationPlugin.asyncPublishAndNotify(channel, msg);
		} else {
			publicationPlugin.publish(channel, msg);
		}
	}
	
	/**
	 * Publishes given {@link WindData}s on the {@code channel}, from the position of the MeteoStation.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code windDatas != null}
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param windDatas 	Wind datas to publish on the {@code channel}.
	 * @param channel		The channel on which it must publish the wind data.
	 * @param notify		Indicates if the publishing service with notification must be called or not.
	 * @throws Exception	If an error occurs while publishing.
	 */
	public void publishWindDatas(List<WindData> windDatas, String channel, boolean notify) throws Exception {
		// Precondition checks
		assert(windDatas != null) : new PreconditionException("windDatas cannot be null");	
		assert(channel != null && !channel.isEmpty()) : new PreconditionException("channel cannot be null or empty");	
		
		ArrayList<MessageI> messages = new ArrayList<>();
		for (WindData wd : windDatas) {
			Message msg = new WindMessage(wd);
			messages.add(msg);
		}
			
		this.logMessage("------------------------------");
		this.logMessage("MeteoStation sending "+messages.size()+" wind datas to channel : '"+channel+"'");
		this.logMessage("------------------------------");
		
		if (notify) {
			publicationPlugin.asyncPublishAndNotify(channel, messages);
		} else {
			publicationPlugin.publish(channel, messages);
		}
	}
	
	/**
	 * Creates a new channel on the broker.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code authorizedUsers != null && !authorizedUsers.isEmpty()}
	 * pre	{@code registrationClass == (RegistrationClass.STANDARD || RegistrationClass.PREMIUM)}
	 * post	{@code publicationPlugin.channelExist(channel)}
	 * </pre>
	 * 
	 * @param channel 			The new channel name.
	 * @param authorizedUsers 	The regex for filtering accepted users.
	 * @throws Exception		If an error occurs while creating the new channel.
	 */
	public void createNewChannel(String channel, String authorizedUsers) throws Exception {
		// Precondition checks
		assert(channel != null && !channel.isEmpty()) : 
			new PreconditionException("channel cannot be null or empty");
		assert(authorizedUsers != null && !authorizedUsers.isEmpty()) : 
			new PreconditionException("authorizedUsers cannot be null or empty");
		assert(registrationClass == RegistrationClass.STANDARD ||  registrationClass == RegistrationClass.PREMIUM) : 
			new PreconditionException("client must be privileged");
		
		privilegedPlugin.createChannel(channel, authorizedUsers);
		this.logMessage("Meteo Bureau has created channel : "+channel);
		
		// Postcondition check
		assert(publicationPlugin.channelExist(channel)) : new PostconditionException("new channel '"+channel+"' should exist");
	}
	
	/**
	 * Modifies authorized users of the {@code channel}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code registrationClass == (RegistrationClass.STANDARD || RegistrationClass.PREMIUM)}
	 * post	{@code !publicationPlugin.channelExist(channel)}
	 * </pre>
	 * 
	 * @param channel 			The channel to destroy.
	 * @param now				Indicates if we destroy NOW the channel, without sending to clients unloaded messages.
	 * @throws Exception		If an error occurs while destroying the channel, or verifying the deletion.
	 */
	public void destroyChannelAndVerifies(String channel, boolean now) throws Exception {
		// Precondition checks
		assert(channel != null && !channel.isEmpty()) : 
			new PreconditionException("channel cannot be null or empty");
		assert(registrationClass == RegistrationClass.STANDARD ||  registrationClass == RegistrationClass.PREMIUM) : 
			new PreconditionException("client must be privileged");
		
		if (now) {
			privilegedPlugin.destroyChannelNow(channel);
		} else {
			privilegedPlugin.destroyChannel(channel);
		}
		
		this.logMessage("Meteo Bureau has destroyed its channel : "+channel);
		
		boolean verif = subscriptionPlugin.channelExist(channel);
		this.logMessage("Channel still exists ? "+verif);
		
		// Postcondition check
		assert(!verif) : new PostconditionException("channel '"+channel+"' should not exist anymore");
	}
}