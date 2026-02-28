package fr.sorbonne_u.meteo.components;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.clients.connectors.ClientPublishingServiceConnector;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI;
import fr.sorbonne_u.cps.meteo.interfaces.RegionI;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.messages.Message;
import fr.sorbonne_u.meteo.data.MeteoAlert;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.meteo.data.RegionData;

/**
 * Class representing a meteorological office emitting random alerts periodically.
 * 
 * <p>
 * The office sends alerts at random intervals within the specified range.
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code bureauPosition != null}
 * invariant	{@code sleepMinTime >= 0}
 * invariant	{@code sleepMaxTime >= sleepMinTime}
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
@RequiredInterfaces(required= {RegistrationCI.class,PublishingCI.class})
@OfferedInterfaces(offered= {ReceivingCI.class})
public class MeteoBureau extends Client {
	
	private PositionData bureauPosition;
	
	// Waiting time between each alert in seconds
	private int sleepMinTime;
	private int sleepMaxTime;

	//--------------------------------------------------------------------------
	// Constructor
	//--------------------------------------------------------------------------

	/**
	 * Creates a meteorological office component of the publish/subscribe system, extending {@link Client}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code registrationOutboundPortURI != null && !registrationOutboundPortURI.isEmpty()}
	 * pre	{@code publishingOutboundPortURI != null && !publishingOutboundPortURI.isEmpty()}
	 * pre	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
	 * pre	{@code bureauPosition != null}
	 * pre	{@code sleepMinTime >= 0 && sleepMaxTime >= sleepMinTime}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param registrationOutboundPortURI   URI of the outbound port requiring {@link RegistrationCI}.
	 * @param publishingOutboundPortURI     URI of the outbound port requiring {@link PublishingCI}.
	 * @param receivingInboundPortURI       URI of the inbound port offering {@link ReceivingCI}.
	 * @param bureauPosition                The position of the office.
	 * @param sleepMinTime                  Minimum waiting time between two alerts.
	 * @param sleepMaxTime                  Maximum waiting time between two alerts.
	 * @throws Exception                    If an error occurs during initialization or port publication.
	 */
	protected MeteoBureau(String registrationOutboundPortURI, String publishingOutboundPortURI, String receivingInboundPortURI, 
			PositionData bureauPosition, int sleepMinTime, int sleepMaxTime) throws Exception {
		super(registrationOutboundPortURI, publishingOutboundPortURI, receivingInboundPortURI);
		assert(bureauPosition != null);
		assert(sleepMinTime >= 0 && sleepMaxTime >= sleepMinTime);
		
		this.bureauPosition = bureauPosition;
		this.sleepMinTime = sleepMinTime;
		this.sleepMaxTime = sleepMaxTime;
		
		this.getTracer().setTitle("MeteoBureau");
		this.getTracer().setRelativePosition(1, 0);
	}

	// --------------------------------------------------------------------------
	// Component life-cycle
	// --------------------------------------------------------------------------

	@Override
	public void execute() throws Exception {
		super.execute();
		this.logMessage("executing MeteoBureau component");
		
		// Register on the Broker if not already registered
		if (!this.registrationOutboundPort.registered(receivingInboundPort.getPortURI())) {
			brokerPublishingPortURI = this.registrationOutboundPort.register(receivingInboundPort.getPortURI(), RegistrationClass.FREE);
		}
		assert (brokerPublishingPortURI != null);
		this.logMessage("MeteoBureau registered");
		
		// Connect the publishing port
		this.doPortConnection(this.publishingOutboundPort.getPortURI(), 
				brokerPublishingPortURI, 
				ClientPublishingServiceConnector.class.getCanonicalName());
		
		// Send alerts periodically on 'channel1'
	    //scheduleNextSend(); // For later, when scheduled threads
		
	    sendAlert("channel1");
	}
	
	// --------------------------------------------------------------------------
	// Alert sending
	// --------------------------------------------------------------------------

	/**
	 * Schedules a new alert after a random interval.
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
	                    ((MeteoBureau) this.getTaskOwner()).sendAlert("channel1");
	                    ((MeteoBureau) this.getTaskOwner()).scheduleNextSend();
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
	 * Sends a {@link MeteoAlert} with random attributes.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param channel      The name of the channel to send the alert on.
	 * @throws Exception   If an error occurs during publishing.
	 */
	private void sendAlert(String channel) throws Exception {
		assert(channel != null && !channel.isEmpty());	
		
		int seconds = ThreadLocalRandom.current().nextInt(sleepMinTime, sleepMaxTime); 
		this.logMessage("MeteoBureau sleeps "+seconds+" seconds..."); 
		Thread.sleep(seconds * 1000L);
		
		Message msg = new Message();
		
		ArrayList<RegionI> regions = new ArrayList<>();
		regions.add(randomRegion());
		
		MeteoAlertI.AlertType alert = randomAlert();
		MeteoAlertI.Level level = randomLevel();
		Instant instant = Instant.now();
		Duration duration = randomDuration();
		MeteoAlert ma = new MeteoAlert(alert, level, regions, instant, duration);
		
		msg.setPayload(ma);
		msg.putProperty("alert", alert);
		msg.putProperty("level", level);
		msg.putProperty("regions", regions);
		msg.putProperty("instant", instant);
		msg.putProperty("duration", duration);
			
		this.logMessage("------------------------------");
		this.logMessage("MeteoBureau sending an alert "+ma);
		this.logMessage("------------------------------");
		
		this.publishingOutboundPort.publish(receivingInboundPort.getPortURI(), channel, msg);
		
		// Recursive call
		sendAlert("channel1");
	}
	
	// --------------------------------------------------------------------------
	// Internal random helpers
	// --------------------------------------------------------------------------

	/**
	 * Creates a random alert type.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @return a random alert type.
	 */
	private MeteoAlertI.AlertType randomAlert() {
		switch (ThreadLocalRandom.current().nextInt(0, 6)) {
	        case 0: return MeteoAlertI.AlertType.STORM;
	        case 1: return MeteoAlertI.AlertType.ICY_STORM;
	        case 2: return MeteoAlertI.AlertType.SNOW_STORM;
	        case 3: return MeteoAlertI.AlertType.FLOODING;
	        case 4: return MeteoAlertI.AlertType.EARTHQUAKE;
	        case 5: return MeteoAlertI.AlertType.ERUPTION;
	        default: throw new AssertionError("Impossible");
	    }
    }
	
	/**
	 * Creates a random alert level.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @return a random alert level.
	 */
	private MeteoAlertI.Level randomLevel() {
		switch (ThreadLocalRandom.current().nextInt(0, 5)) {
	        case 0: return MeteoAlertI.Level.GREEN;
	        case 1: return MeteoAlertI.Level.YELLOW;
	        case 2: return MeteoAlertI.Level.ORANGE;
	        case 3: return MeteoAlertI.Level.RED;
	        case 4: return MeteoAlertI.Level.SCARLET;
	        default: throw new AssertionError("Impossible");
	    }
    }
	
	/**
	 * Creates a random {@link RegionData}.
	 * 
	 * <p>The region created has a 3 x 3 size, with minimum X/Y in [0, 6[.</p> 
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @return a random {@link RegionData}.
	 */
	private RegionData randomRegion() {
		double xMin = ThreadLocalRandom.current().nextInt(0, 6);
		double xMax = xMin + 3;
		double yMin = ThreadLocalRandom.current().nextInt(0, 6);
		double yMax = yMin + 3;
		return new RegionData(xMin, xMax, yMin, yMax);
    }

	/**
	 * Creates a random {@link Duration}.
	 * 
	 * <p>The duration is between 5 and 60 seconds</p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @return a random {@link Duration}.
	 */
	private Duration randomDuration() {
	    int seconds = ThreadLocalRandom.current().nextInt(5, 61);
	    return Duration.ofSeconds(seconds);
	}
}