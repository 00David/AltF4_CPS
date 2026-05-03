package fr.sorbonne_u.meteo.components;

import java.util.ArrayList;
import java.util.List;

import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.components.utils.tests.TestScenario;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.exceptions.PostconditionException;
import fr.sorbonne_u.exceptions.PreconditionException;
import fr.sorbonne_u.messages.Message;
import fr.sorbonne_u.messages.meteo.WindMessage;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.meteo.data.WindData;
import fr.sorbonne_u.utils.aclocks.ClocksServer;

/**
 * Class representing a meteorological station able to send winds and to participate in a given test scenario.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code this.subscriptionPlugin != null && this.subscriptionPlugin.isInitialised()}
 * invariant	{@code testScenario != null}
 * invariant	the Client has exactly 1 published inbound port implementing ReceivingCI. // informal
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class MeteoStation extends Client {
	
	/** The component position. */
	protected final PositionData stationPosition;
	
	/** The test scenario followed by the component. */
	protected final TestScenario testScenario;
	
	// --------------------------------------------------------------------------
	// Constructor
	// --------------------------------------------------------------------------
	
	/**
	 * Creates a test meteorological station component of the publish/subscribe system, extending {@link Client}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code reflectionInboundPortURI != null && !reflectionInboundPortURI.isEmpty()}
	 * pre	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
     * pre	{@code rc != null}
	 * pre	{@code nbThreads >= 0 && nbSchedulableThreads >= 0}
	 * pre	{@code stationPosition != null}
	 * pre  {@code testScenario != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param id							A number identifying the component, just for tracer window.
	 * @param reflectionInboundPortURI		URI of the reflection inbound port to be created in this component.
	 * @param receivingInboundPortURI       URI of the inbound port offering {@link ReceivingCI}.
	 * @param rc			   				The registration class of the component.
	 * @param nbThreads					  	Number of threads to be created in the component pool.
	 * @param nbSchedulableThreads		  	Number of threads to be created in the component schedulable pool.
	 * @param stationPosition               The position of the station.
	 * @param testScenario				  	Test scenario in which this component participate.
	 * @throws Exception                    If an error occurs during initialization or port publication.
	 */
	protected MeteoStation(int id, String reflectionInboundPortURI, String receivingInboundPortURI, RegistrationClass rc, 
			int nbThreads, int nbSchedulableThreads, PositionData stationPosition, TestScenario testScenario) throws Exception {
		super(reflectionInboundPortURI, receivingInboundPortURI, rc, nbThreads, nbSchedulableThreads);
		
		// Some preconditions are checked inside the super-constructor while others are checked here
		assert(stationPosition != null) : 
			new PreconditionException("stationPosition cannot be null");	
		assert (testScenario != null) : 
			new PreconditionException("testScenario cannot be null");	

		this.stationPosition = stationPosition;
		this.testScenario = testScenario;
		
		// Install the subscription + default reception plug-in
		setupSubscriptionPlugin();
		
		this.getTracer().setTitle("MeteoStation "+id);
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