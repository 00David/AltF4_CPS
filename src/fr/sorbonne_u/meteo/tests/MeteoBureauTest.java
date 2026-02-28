package fr.sorbonne_u.meteo.tests;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;

import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.clients.connectors.ClientPublishingServiceConnector;
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
 * Test class representing a {@link MeteoBureau} following a predefined scenario.
 * 
 * <p>
 * The scenario tested:
 * <li> - The weather office sleeps during the first 90 seconds of the program.
 * <li> - It sends a dangerous alert (RED level) to 'channel1' lasting 30 seconds.
 * <li> - It then sleeps for 30 seconds during the alert.
 * <li> - It wakes up and sends a safe alert (GREEN level) indicating the end of danger.
 * <li> - It then sleeps for 60 seconds.
 * <li> - It wakes up and sends a restricted alert limited to the office's area, not affecting the wind turbine.
 * </p>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
@RequiredInterfaces(required= {RegistrationCI.class,PublishingCI.class})
@OfferedInterfaces(offered= {ReceivingCI.class})
public class MeteoBureauTest extends Client{
	
	private PositionData bureauPosition;
	private String channel = "channel1";
	private int sleepSinceStart = 90; // in seconds
	private int durationAlertTime = 30; // in seconds
	private int sleepAfterSafeAlert = 60; // in seconds

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
	 * pre	{@code bureauPosition != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param registrationOutboundPortURI   URI of the outbound port requiring {@link RegistrationCI}.
	 * @param publishingOutboundPortURI     URI of the outbound port requiring {@link PublishingCI}.
	 * @param receivingInboundPortURI       URI of the inbound port offering {@link ReceivingCI}.
	 * @param bureauPosition                The position of the office.
	 * @throws Exception                    If an error occurs during initialization or port publication.
	 */
	protected MeteoBureauTest(String registrationOutboundPortURI, String publishingOutboundPortURI, String receivingInboundPortURI, 
			PositionData bureauPosition) throws Exception {
		super(registrationOutboundPortURI, publishingOutboundPortURI, receivingInboundPortURI);
		assert(bureauPosition != null);
		
		this.bureauPosition = bureauPosition;
		
		this.getTracer().setTitle("MeteoBureau") ;
		this.getTracer().setRelativePosition(1, 0) ;
	}

	// --------------------------------------------------------------------------
	// Component life-cycle
	// --------------------------------------------------------------------------

	@Override
	public void execute() throws Exception {
		super.execute();
		this.logMessage("executing MeteoBureau component");
		
		// If not registered on the Broker
		if (!this.registrationOutboundPort.registered(receivingInboundPort.getPortURI())) {
			// Register and get the Broker's publishing inbound port URI
			brokerPublishingPortURI = this.registrationOutboundPort.register(receivingInboundPort.getPortURI(), RegistrationClass.FREE);
		}
		assert (brokerPublishingPortURI != null);
		this.logMessage("MeteoBureau registered");
		
		// Connect the publishing port
		this.doPortConnection(this.publishingOutboundPort.getPortURI(), 
				brokerPublishingPortURI, 
				ClientPublishingServiceConnector.class.getCanonicalName());
		
		sendAlertDanger();
		sendAlertSafe();
		sendUncaughtAlert();
	}
	
	// --------------------------------------------------------------------------
	// Sending alerts
	// --------------------------------------------------------------------------

	/**
	 * Sends an alert indicating the start of a dangerous alert.
	 * @throws Exception		If an error occurs during publishing.
	 */
	public void sendAlertDanger() throws Exception {

		// Sleep for a certain number of seconds since start
		// Then send a dangerous alert
		this.logMessage("MeteoBureau sleeps "+sleepSinceStart+" seconds..."); 
		Thread.sleep(sleepSinceStart * 1000L);
		
		Message msg = new Message();
		
		ArrayList<RegionI> regions = new ArrayList<>();
		double x = bureauPosition.getPosX(); double y = bureauPosition.getPosY();
		regions.add(new RegionData(x-1, x+1, y-1, y+1)); // 3x3 zone around the office
		MeteoAlertI.AlertType alert = MeteoAlertI.AlertType.EARTHQUAKE;
		MeteoAlertI.Level level = MeteoAlertI.Level.RED;
		Instant instant = Instant.now(); // start instant
		Duration duration = Duration.ofSeconds(durationAlertTime);
		MeteoAlert ma = new MeteoAlert(alert, level, regions, instant, duration);
		
		msg.setPayload(ma);
		msg.putProperty("alert", alert);
		msg.putProperty("level", level);
		msg.putProperty("regions", regions);
		msg.putProperty("instant", instant);
		msg.putProperty("duration", duration);
			
		this.logMessage("------------------------------");
		this.logMessage("MeteoBureau sending a DANGER alert "+ma);
		this.logMessage("------------------------------");
		
		this.publishingOutboundPort.publish(receivingInboundPort.getPortURI(), channel, msg);
	}
	
	/**
	 * Sends an alert indicating the end of the previous danger alert.
	 * @throws Exception		If an error occurs during publishing.
	 */
	public void sendAlertSafe() throws Exception {
		
		// Sleep for the duration of the dangerous alert
		this.logMessage("MeteoBureau sleeps "+durationAlertTime+" seconds..."); 
		Thread.sleep(durationAlertTime * 1000L);
		
		Message msg = new Message();
		
		ArrayList<RegionI> regions = new ArrayList<>();
		double x = bureauPosition.getPosX(); double y = bureauPosition.getPosY();
		regions.add(new RegionData(x-1, x+1, y-1, y+1)); // 3x3 zone
		MeteoAlertI.AlertType alert = MeteoAlertI.AlertType.EARTHQUAKE;
		MeteoAlertI.Level level = MeteoAlertI.Level.GREEN;
		Instant instant = Instant.now();
		Duration duration = Duration.ofSeconds(durationAlertTime);
		MeteoAlert ma = new MeteoAlert(alert, level, regions, instant, duration);
		
		msg.setPayload(ma);
		msg.putProperty("alert", alert);
		msg.putProperty("level", level);
		msg.putProperty("regions", regions);
		msg.putProperty("instant", instant);
		msg.putProperty("duration", duration);
			
		this.logMessage("------------------------------");
		this.logMessage("MeteoBureau sending a SAFE alert "+ma);
		this.logMessage("------------------------------");
		
		this.publishingOutboundPort.publish(receivingInboundPort.getPortURI(), channel, msg);
	}
	
	/**
	 * Sends an alert not received by the wind turbine, after filtering the concerned region.
	 * @throws Exception		If an error occurs during publishing.
	 */
	public void sendUncaughtAlert() throws Exception {
		
		// Sleep for a certain number of seconds after the safe alert
		this.logMessage("MeteoBureau sleeps "+sleepAfterSafeAlert+" seconds..."); 
		Thread.sleep(sleepAfterSafeAlert * 1000L);
		
		Message msg = new Message();
		
		ArrayList<RegionI> regions = new ArrayList<>();
		double x = bureauPosition.getPosX(); double y = bureauPosition.getPosY();
		regions.add(new RegionData(x, x, y, y)); // zone exactly on the MeteoBureau
		MeteoAlertI.AlertType alert = MeteoAlertI.AlertType.EARTHQUAKE;
		MeteoAlertI.Level level = MeteoAlertI.Level.RED;
		Instant instant = Instant.now();
		Duration duration = Duration.ofSeconds(durationAlertTime);
		MeteoAlert ma = new MeteoAlert(alert, level, regions, instant, duration);
		
		msg.setPayload(ma);
		msg.putProperty("alert", alert);
		msg.putProperty("level", level);
		msg.putProperty("regions", regions);
		msg.putProperty("instant", instant);
		msg.putProperty("duration", duration);
			
		this.logMessage("------------------------------");
		this.logMessage("MeteoBureau sending a DANGER alert "+ma);
		this.logMessage("------------------------------");
		
		this.publishingOutboundPort.publish(receivingInboundPort.getPortURI(), channel, msg);
	}
}