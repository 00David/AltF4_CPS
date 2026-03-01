package fr.sorbonne_u.meteo.tests;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI;
import fr.sorbonne_u.meteo.components.WindTurbine;
import fr.sorbonne_u.meteo.data.PositionData;

/**
 * Test class to simulate the following scenario from the start of the CVM:
 * <p>
 * <li> - After 30 seconds, {@link MeteoStationTest1} sends wind data to 'channel0'.
 * <li> - After 60 seconds, {@link MeteoStationTest2} sends wind data to 'channel0'.
 * <li> - After 90 seconds, {@link MeteoBureauTest} sends a dangerous alert to 'channel1'.
 * <li> - After 120 seconds, {@link MeteoBureauTest} sends an end-of-danger alert to 'channel1'.
 * <li> - After 150 seconds, {@link MeteoStationTest1} sends wind data again to 'channel0'.
 * <li> - After 180 seconds, {@link MeteoBureauTest} sends a danger alert to 'channel1', not affecting the wind turbine region.
 * </p>
 * 
 * <p>
 * The CVM stops after 200 seconds, if not stopped earlier.
 * </p>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class CVMTest extends AbstractCVM {

	// MeteoBureau
	protected final String MBRegistrationOutboundPortURI;
	protected final String MBPublishingOutboundPortURI;
	protected final String MBReceivingInboundPortURI;
	
	// MeteoStation 1
	protected final String MS1RegistrationOutboundPortURI;
	protected final String MS1PublishingOutboundPortURI;
	protected final String MS1ReceivingInboundPortURI;
	
	// MeteoStation 2
	protected final String MS2RegistrationOutboundPortURI;
	protected final String MS2PublishingOutboundPortURI;
	protected final String MS2ReceivingInboundPortURI;
	
	// WindTurbine
	protected final String WindTurbineRegistrationOutboundPortURI;
	protected final String WindTurbinePublishingOutboundPortURI;
	protected final String WindTurbineReceivingInboundPortURI;
	
	// Component URIs
	protected String uriBroker;
	protected String uriMB;
	protected String uriMS1;
	protected String uriMS2;
	protected String uriWindTurbine;
	
	public CVMTest() throws Exception{
		super();
		MBRegistrationOutboundPortURI = "registration-outbound-port-MB";
		MBPublishingOutboundPortURI = "publishing-outbound-port-MB";
		MBReceivingInboundPortURI = "inbound-port-client-MB";
		
		MS1RegistrationOutboundPortURI = "registration-outbound-port-MS1";
		MS1PublishingOutboundPortURI = "publishing-outbound-port-MS1";
		MS1ReceivingInboundPortURI = "inbound-port-client-MS1";
		
		MS2RegistrationOutboundPortURI = "registration-outbound-port-MS2";
		MS2PublishingOutboundPortURI = "publishing-outbound-port-MS2";
		MS2ReceivingInboundPortURI = "inbound-port-client-MS2";
		
		WindTurbineRegistrationOutboundPortURI = "registration-outbound-port-WindTurbine";
		WindTurbinePublishingOutboundPortURI = "publishing-outbound-port-WindTurbine";
		WindTurbineReceivingInboundPortURI = "inbound-port-client-WindTurbine";
	}
	
	@Override
	public void	deploy() throws Exception{
		assert	!this.deploymentDone() ;
		
		// ---------------------------------------------------------------------
		// Creation phase
		// ---------------------------------------------------------------------
		
		// Broker creation
		int nbMaxChannelsSTD = 2;
		int nbMaxChannelsPREM = 4;
		this.uriBroker = AbstractComponent.createComponent(
				Broker.class.getCanonicalName(),
				new Object[] {2, nbMaxChannelsSTD, nbMaxChannelsPREM});
		assert this.isDeployedComponent(this.uriBroker);
		this.toggleTracing(this.uriBroker);

		
		// Creation of 1 MeteoBureau
		PositionData bureauPosition = new PositionData(2, 1);
		this.uriMB = AbstractComponent.createComponent(
				MeteoBureauTest.class.getCanonicalName(),
				new Object[] { MBRegistrationOutboundPortURI, MBPublishingOutboundPortURI, MBReceivingInboundPortURI,
						bureauPosition});
		assert this.isDeployedComponent(this.uriMB);
		this.toggleTracing(this.uriMB);
		
		// Creation of 2 MeteoStation
		PositionData station1Position = new PositionData(1, 2);
		this.uriMS1 = AbstractComponent.createComponent(
				MeteoStationTest1.class.getCanonicalName(),
				new Object[] { MS1RegistrationOutboundPortURI, MS1PublishingOutboundPortURI, MS1ReceivingInboundPortURI,
						station1Position});
		assert this.isDeployedComponent(this.uriMS1);
		this.toggleTracing(this.uriMS1);
	
		PositionData station2Position = new PositionData(3, 2);
		this.uriMS2 = AbstractComponent.createComponent(
				MeteoStationTest2.class.getCanonicalName(),
				new Object[] { MS2RegistrationOutboundPortURI, MS2PublishingOutboundPortURI, MS2ReceivingInboundPortURI,
						station2Position});
		assert this.isDeployedComponent(this.uriMS2);
		this.toggleTracing(this.uriMS2);
		
		// Creation of 1 WindTurbine between both MeteoStation
		PositionData windTurbinePosition = new PositionData(2, 2);
		this.uriWindTurbine = AbstractComponent.createComponent(WindTurbine.class.getCanonicalName(),
				new Object[] {WindTurbineRegistrationOutboundPortURI,WindTurbinePublishingOutboundPortURI,WindTurbineReceivingInboundPortURI,
						windTurbinePosition, MeteoAlertI.Level.ORANGE});
		assert this.isDeployedComponent(uriWindTurbine);
		this.toggleTracing(uriWindTurbine);

		// --------------------------------------------------------------------
		// Deployment done
		// --------------------------------------------------------------------

		super.deploy();
		assert this.deploymentDone();
	}
	
	public static void main(String[] args) {
		try {
			CVMTest a = new CVMTest() ;
			
			int duree = 200; // in seconds
			a.startStandardLifeCycle(duree * 1000L) ; // start

			System.exit(0) ;
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
}
