package fr.sorbonne_u.meteo.components;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;

import fr.sorbonne_u.clients.component.Client;
import fr.sorbonne_u.components.utils.tests.TestScenario;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI;
import fr.sorbonne_u.cps.meteo.interfaces.RegionI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.exceptions.PostconditionException;
import fr.sorbonne_u.exceptions.PreconditionException;
import fr.sorbonne_u.messages.Message;
import fr.sorbonne_u.messages.meteo.AlertMessage;
import fr.sorbonne_u.meteo.data.MeteoAlert;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.utils.aclocks.ClocksServer;

/**
 * Class representing a meteorological office, able to send meteorological alerts and to participate in a given test scenario.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code bureauPosition != null}
 * invariant	{@code testScenario != null}
 * invariant	{@code this.subscriptionPlugin != null && this.subscriptionPlugin.isInitialised()}
 * invariant	the Client has exactly 1 published inbound port implementing ReceivingCI. // informal
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class MeteoBureau extends Client {
	
	/** The component position. */
	protected final PositionData bureauPosition;
	
	/** The test scenario followed by the component. */
	protected final TestScenario testScenario;

	//--------------------------------------------------------------------------
	// Constructor
	//--------------------------------------------------------------------------

	/**
	 * Creates a test meteorological office component for the publish/subscribe system, extending {@link Client}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code reflectionInboundPortURI != null && !reflectionInboundPortURI.isEmpty()}
	 * pre	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
	 * pre	{@code nbThreads >= 0 && nbSchedulableThreads >= 0}
	 * pre	{@code bureauPosition != null}
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
	 * @param bureauPosition                The position of the office.
	 * @param testScenario					Test scenario in which this component participate.
	 * @throws Exception                    If an error occurs during initialization or port publication.
	 */
	protected MeteoBureau(int id, String reflectionInboundPortURI, String receivingInboundPortURI, RegistrationClass rc, int nbThreads, int nbSchedulableThreads, 
			PositionData bureauPosition, TestScenario testScenario) throws Exception {
		super(reflectionInboundPortURI, rc, nbThreads, nbSchedulableThreads);
		
		// Some preconditions are checked inside the super-constructor while others are checked here
		assert(bureauPosition != null) : 
			new PreconditionException("bureauPosition cannot be null");
		assert (testScenario != null) : 
			new PreconditionException("testScenario cannot be null");	
		
		this.bureauPosition = bureauPosition;
		this.testScenario = testScenario;
		
		// Install the subscription + default reception plug-in
		setupSubscriptionPlugin(receivingInboundPortURI);
		
		this.getTracer().setTitle("MeteoBureau "+id);
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
	 * Publishes an alert on the given {@code channel}, with the given parameters.
	 * 
	 * <p><strong>Contract</strong></p>
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
		
		MeteoAlert ma = new MeteoAlert(type, level, regions, instant, duration);
		Message msg = new AlertMessage(ma);
			
		this.logMessage("------------------------------");
		this.logMessage("MeteoBureau sending a "+level+" alert "+ma+"\nto channel : '"+channel+"'");
		this.logMessage("------------------------------");
		
		publicationPlugin.publish(channel, msg);
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
	 * pre	{@code authorizedUsers != null && !authorizedUsers.isEmpty()}
	 * pre	{@code registrationClass == (RegistrationClass.STANDARD || RegistrationClass.PREMIUM)}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param channel 			The channel name.
	 * @param authorizedUsers 	The new regex for filtering accepted users.
	 * @throws Exception		If an error occurs while modifying the authorized users channel.
	 */
	public void modifyAuthorizedUsers(String channel, String authorizedUsers) throws Exception {
		// Precondition checks
		assert(channel != null && !channel.isEmpty()) : 
			new PreconditionException("channel cannot be null or empty");
		assert(authorizedUsers != null && !authorizedUsers.isEmpty()) : 
			new PreconditionException("authorizedUsers cannot be null or empty");
		assert(registrationClass == RegistrationClass.STANDARD ||  registrationClass == RegistrationClass.PREMIUM) : 
			new PreconditionException("client must be privileged");
		
		privilegedPlugin.modifyAuthorisedUsers(channel, authorizedUsers);
		this.logMessage("Meteo Bureau has modified authorized users on channel : "+channel+", now authorized : "+authorizedUsers);
	}
	
	/**
	 * Destroys the given {@code channel} and verifies that it has been deleted.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * pre	{@code registrationClass == (RegistrationClass.STANDARD || RegistrationClass.PREMIUM)}
	 * post	{@code !publicationPlugin.channelExist(channel)}
	 * </pre>
	 * 
	 * @param channel 			The channel to destroy.
	 * @throws Exception		If an error occurs while destroying the channel, or verifying the deletion.
	 */
	public void destroyChannelAndVerifies(String channel) throws Exception {
		// Precondition checks
		assert(channel != null && !channel.isEmpty()) : 
			new PreconditionException("channel cannot be null or empty");
		assert(registrationClass == RegistrationClass.STANDARD ||  registrationClass == RegistrationClass.PREMIUM) : 
			new PreconditionException("client must be privileged");
		
		privilegedPlugin.destroyChannel(channel);
		this.logMessage("Meteo Bureau has destroyed its channel : "+channel);
		
		boolean verif = subscriptionPlugin.channelExist(channel);
		this.logMessage("Channel still exists ? "+verif);
		
		// Postcondition check
		assert(!verif) : new PostconditionException("channel '"+channel+"' should not exist anymore");
	}
}