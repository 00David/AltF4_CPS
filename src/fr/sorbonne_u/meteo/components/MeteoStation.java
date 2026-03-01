package fr.sorbonne_u.meteo.components;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.clients.plugins.ClientPublicationPlugin;
import fr.sorbonne_u.clients.plugins.ClientSubscriptionPlugin;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.messages.Message;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.meteo.data.WindData;

/**
 * Class representing a meteorological station emitting random wind data periodically.
 * 
 * <p>
 * The station generates wind vectors within a specified range for X and Y components.
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code sleepMinTime >= 0}
 * invariant	{@code sleepMaxTime >= sleepMinTime}
 * invariant	{@code publishingOutboundPortURI!= null & !publishingOutboundPortURI.isEmpty()}
 * invariant	{@code subscriptionPlugin != null}
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class MeteoStation extends Client {
	
	private PositionData stationPosition;

	// Waiting time between each WindData in seconds
	private int sleepMinTime;
	private int sleepMaxTime;
	
	// Bounds for generating X and Y components of wind vectors
	private double windMinXY;
	private double windMaxXY;
	
	// Publication
	protected final String publishingOutboundPortURI;
	protected ClientPublicationPlugin publicationPlugin;
	
	// Subscription + reception
	protected final ClientSubscriptionPlugin subscriptionPlugin;
	
	// --------------------------------------------------------------------------
	// Constructor
	// --------------------------------------------------------------------------

	/**
	 * Creates a meteorological station component for the publish/subscribe system, extending a {@link Client}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre  {@code registrationOutboundPortURI != null && !registrationOutboundPortURI.isEmpty()}
	 * pre  {@code publishingOutboundPortURI != null && !publishingOutboundPortURI.isEmpty()}
	 * pre  {@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
	 * pre  {@code stationPosition != null}
	 * pre  {@code sleepMinTime >= 0 && sleepMaxTime >= sleepMinTime}
	 * pre  {@code windMaxXY >= windMinXY}
	 * post {@code true} // no postcondition.
	 * </pre>
	 * 
	 * @param registrationOutboundPortURI URI of the outbound port requesting {@link RegistrationCI}.
	 * @param publishingOutboundPortURI   URI of the outbound port requesting {@link PublishingCI}.
	 * @param receivingInboundPortURI     URI of the inbound port providing {@link ReceivingCI}.
	 * @param stationPosition             The position of the station.
	 * @param sleepMinTime                Minimum wait time between each new {@link WindData} sending.
	 * @param sleepMaxTime                Maximum wait time between each new {@link WindData} sending.
	 * @param windMinXY                   Minimum X value for a new wind vector {@link WindData}.
	 * @param windMaxXY                   Maximum X value for a new wind vector {@link WindData}.
	 * @throws Exception                  If a problem occurs during initialization or port publication.
	 */
	protected MeteoStation(String registrationOutboundPortURI, String publishingOutboundPortURI, String receivingInboundPortURI, 
			PositionData stationPosition, int sleepMinTime, int sleepMaxTime, double windMinXY, double windMaxXY) throws Exception {
		super(receivingInboundPortURI, registrationOutboundPortURI, RegistrationClass.FREE);
		assert(stationPosition != null);
		assert(sleepMinTime >= 0 && sleepMaxTime >= sleepMinTime);
		assert(windMaxXY >= windMinXY);

		this.stationPosition = stationPosition;
		this.sleepMinTime = sleepMinTime;
		this.sleepMaxTime = sleepMaxTime;
		this.windMinXY = windMinXY;
		this.windMaxXY = windMaxXY;
		this.publishingOutboundPortURI = publishingOutboundPortURI;
		
		// Install the subscription + reception plugin
		this.subscriptionPlugin = new ClientSubscriptionPlugin(receivingInboundPortURI, registrationPlugin.getRegistrationOutboundPort());
		this.subscriptionPlugin.setPluginURI("subscription");
		this.installPlugin(this.subscriptionPlugin);
		
		this.getTracer().setTitle("MeteoStation");
		this.getTracer().setRelativePosition(2, 0);
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
		
		// Send WindData periodically on 'channel0'
		//scheduleNextSend(); // For later, when scheduled threads
		
		sendWindData("channel0");
	}
	
	// --------------------------------------------------------------------------
	// Periodic sending
	// --------------------------------------------------------------------------

	/**
	 * Schedules a new {@link WindData} with random X/Y components in the range [windMinXY, windMaxXY].
	 * 
	 * <p>Currently unused. Will be used when at least one schedulable thread exists.</p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 */
	private void scheduleNextSend() {
	    int seconds = ThreadLocalRandom.current().nextInt(sleepMinTime, sleepMaxTime);

	    this.scheduleTask(
	        new AbstractComponent.AbstractTask() {
	            @Override
	            public void run() {
	                try {
	                    ((MeteoStation) this.getTaskOwner()).sendWindData("channel0");
	                    ((MeteoStation) this.getTaskOwner()).scheduleNextSend();
	                } catch (Throwable e) {
	                    e.printStackTrace();
	                }
	            }
	        },
	        seconds,
	        TimeUnit.SECONDS
	    );
	}
	
	/**
	 * Sends a {@link WindData} with random X/Y components in the range [windMinXY, windMaxXY].
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 */
	private void sendWindData(String channel) throws Exception {
		assert(channel != null && !channel.isEmpty());
		
		int seconds = ThreadLocalRandom.current().nextInt(sleepMinTime, sleepMaxTime); 
		this.logMessage("MeteoStation sleeps "+seconds+" seconds..."); 
		Thread.sleep(seconds * 1000L);
		
		Message msg = new Message();
		double xWind = ThreadLocalRandom.current().nextDouble(windMinXY, windMaxXY);
		double yWind = ThreadLocalRandom.current().nextDouble(windMinXY, windMaxXY);
		WindData wd = new WindData(stationPosition, xWind, yWind);
		
		msg.setPayload(wd);
		msg.putProperty("position", stationPosition);
		msg.putProperty("xWind", xWind);
		msg.putProperty("yWind", yWind);
		
		this.logMessage("------------------------------");
		this.logMessage("MeteoStation sending wind data "+wd);
		this.logMessage("------------------------------");
		
		publicationPlugin.publish(channel, msg);
		
		// Recursive call
		sendWindData("channel0");
	}
}