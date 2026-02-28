package fr.sorbonne_u.meteo.tests;

import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.clients.connectors.ClientPublishingServiceConnector;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.messages.Message;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.meteo.data.WindData;

/**
 * Class representing a {@link MeteoStation} emitting wind data following a predefined scenario.
 * 
 * <p>
 * The scenario:
 * <li> - The component simply waits 60 seconds after its launch and sends a {@link WindData} to 'channel0'.
 * </p>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
@RequiredInterfaces(required= {RegistrationCI.class,PublishingCI.class})
@OfferedInterfaces(offered= {ReceivingCI.class})
public class MeteoStationTest2 extends Client {
	
	private PositionData stationPosition;
	private String channel = "channel0";
	private int sleepSinceStart = 60; // in seconds
	
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
	 * pre	{@code stationPosition != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param registrationOutboundPortURI   URI of the outbound port requiring {@link RegistrationCI}.
	 * @param publishingOutboundPortURI     URI of the outbound port requiring {@link PublishingCI}.
	 * @param receivingInboundPortURI       URI of the inbound port offering {@link ReceivingCI}.
	 * @param stationPosition               The position of the station.
	 * @throws Exception                    If an error occurs during initialization or port publication.
	 */
	protected MeteoStationTest2(String registrationOutboundPortURI, String publishingOutboundPortURI, String receivingInboundPortURI, 
			PositionData stationPosition) throws Exception {
		super(registrationOutboundPortURI, publishingOutboundPortURI, receivingInboundPortURI);
		assert(stationPosition != null);

		this.stationPosition = stationPosition;
		
		this.getTracer().setTitle("MeteoStation 2");
		this.getTracer().setRelativePosition(3, 0);
	}
	
	// --------------------------------------------------------------------------
	// Component life-cycle
	// --------------------------------------------------------------------------

	@Override
	public void execute() throws Exception {
		super.execute();
		this.logMessage("executing MeteoStation component");
		
		// If not registered on the Broker
		if (!this.registrationOutboundPort.registered(receivingInboundPort.getPortURI())) {
			// Register and get the Broker's publishing inbound port URI
			brokerPublishingPortURI = this.registrationOutboundPort.register(receivingInboundPort.getPortURI(), RegistrationClass.FREE);
		}
		assert (brokerPublishingPortURI != null);
		this.logMessage("MeteoStation registered");
		
		// Connect the publishing port
		this.doPortConnection(this.publishingOutboundPort.getPortURI(), 
				brokerPublishingPortURI, 
				ClientPublishingServiceConnector.class.getCanonicalName());
		
	    sendWindData();
	}
	
	// --------------------------------------------------------------------------
	// Sending a WindData
	// --------------------------------------------------------------------------

	/**
	 * Sends a predefined {@link WindData} after {@code sleepSinceStart} seconds.
	 * @throws Exception		If an error occurs during publishing.
	 */
	private void sendWindData() throws Exception {
		
		this.logMessage("MeteoStation sleeps "+sleepSinceStart+" seconds..."); 
		Thread.sleep(sleepSinceStart * 1000L);
		
		Message msg = new Message();
		// X and Y components of the wind vector
		double xWind = 1.0;
		double yWind = 1.0;
		WindData wd = new WindData(stationPosition, xWind, yWind);

		msg.setPayload(wd);
		msg.putProperty("position", stationPosition);
		msg.putProperty("xWind", xWind);
		msg.putProperty("yWind", yWind);
			
		this.logMessage("------------------------------");
		this.logMessage("MeteoStation sending wind data "+wd);
		this.logMessage("------------------------------");
		
		this.publishingOutboundPort.publish(receivingInboundPort.getPortURI(), channel, msg);
	}
}