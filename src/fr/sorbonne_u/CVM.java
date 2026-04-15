package fr.sorbonne_u;

import java.util.ArrayList;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.meteo.tests.audit1.CVMSimpleTest;
import fr.sorbonne_u.meteo.tests.audit2.CVMAbnormalConcurrentPublicationsTest;
import fr.sorbonne_u.meteo.tests.audit2.CVMAsynchronousClientReceptionTest;
import fr.sorbonne_u.meteo.tests.audit2.CVMConcurrentPublicationsTest;
import fr.sorbonne_u.meteo.tests.mid_semester.CVMCompleteTest;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.meteo.components.MeteoBureau;
import fr.sorbonne_u.meteo.components.MeteoStation;
import fr.sorbonne_u.meteo.components.WindTurbine;
import fr.sorbonne_u.meteo.data.PositionData;

/**
 * CVM for launching the program with client components publishing random messages on the broker.
 * 
 * <p>
 * 	For more complete/useful usages of the system, please see test classes :
 * </p>
 * 	<ul>
 * 		<li>{@link CVMSimpleTest}, providing a simple prepared scenario of the system usage.</li>
 * 		<li>{@link CVMCompleteTest}, providing a more complex prepared scenario of the system usage, 
 * 			with components using different filters and privileged features.</li>
 * 		<li>{@link CVMConcurrentPublicationsTest}, providing a prepared scenario of the system usage
 * 			with concurrent publications.</li>
 * 		<li>{@link CVMAbnormalConcurrentPublicationsTest}, providing a prepared scenario for errors management
 * 			while doing concurrent publications.</li>
 * 		<li>{@link CVMAsynchronousClientReceptionTest}, providing a prepared scenario for testing every
 * 			asynchronous client reception method.</li>
 * 	</ul>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class CVM extends AbstractCVM {

	// MeteoBureau
	protected final String MBRegistrationOutboundPortURI = "registration-outbound-port-MB";
	protected final String MBPublishingOutboundPortURI = "publishing-outbound-port-MB";;
	protected final String MBReceivingInboundPortURI = "inbound-port-client-MB";
	
	// MeteoStation 1
	protected final String MS1RegistrationOutboundPortURI = "registration-outbound-port-MS1";
	protected final String MS1PublishingOutboundPortURI = "publishing-outbound-port-MS1";
	protected final String MS1ReceivingInboundPortURI = "inbound-port-client-MS1";
	
	// MeteoStation 2
	protected final String MS2RegistrationOutboundPortURI = "registration-outbound-port-MS2";
	protected final String MS2PublishingOutboundPortURI = "publishing-outbound-port-MS2";
	protected final String MS2ReceivingInboundPortURI = "inbound-port-client-MS2";
	
	// WindTurbine
	protected final String WindTurbineRegistrationOutboundPortURI = "registration-outbound-port-WindTurbine";
	protected final String WindTurbinePublishingOutboundPortURI = "publishing-outbound-port-WindTurbine";
	protected final String WindTurbineReceivingInboundPortURI ="inbound-port-client-WindTurbine";
	
	// Component URIs
	protected String uriBroker;
	protected String uriMB;
	protected String uriMS1;
	protected String uriMS2;
	protected String uriWindTurbine;
	
	public CVM() throws Exception{
		super();
	}
	
	@Override
	public void	deploy() throws Exception{
		assert	!this.deploymentDone() ;
		// ---------------------------------------------------------------------
		// Configuration phase
		// ---------------------------------------------------------------------

		/*AbstractCVM.DEBUG_MODE.add(CVMDebugModes.LIFE_CYCLE);
		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.INTERFACES);
		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.PORTS);
		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.CONNECTING);
		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.CALLING);
		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.EXECUTOR_SERVICES);*/
		
		// ---------------------------------------------------------------------
		// Creation phase
		// ---------------------------------------------------------------------
		
		// Broker creation
		int nbMaxChannelsSTD = 2;
		int nbMaxChannelsPREM = 4;
		this.uriBroker = AbstractComponent.createComponent(
				Broker.class.getCanonicalName(),
				new Object[] {1, 2, nbMaxChannelsSTD, nbMaxChannelsPREM, 1, 0, 1, 1, 1, 
						1, "BrokerGossipInboundPortURI", new ArrayList<>(), new ArrayList<>()}); // Gossip protocol unused here
		assert this.isDeployedComponent(this.uriBroker);
		this.toggleTracing(this.uriBroker);

		
		// Creation of 1 MeteoBureau
		PositionData bureauPosition = new PositionData(2, 1);
		int sleepMinTimeMB = 20;
		int sleepMaxTimeMB = 30;
		this.uriMB = AbstractComponent.createComponent(
				MeteoBureau.class.getCanonicalName(),
				new Object[] { MBReceivingInboundPortURI, MBRegistrationOutboundPortURI, MBPublishingOutboundPortURI, "", // privileged port URI empty
						RegistrationClass.FREE, 1, 0, bureauPosition, sleepMinTimeMB, sleepMaxTimeMB});
		assert this.isDeployedComponent(this.uriMB);
		this.toggleTracing(this.uriMB);
		
		// Creation of 2 MeteoStation
		int sleepMinTimeMS = 5;
		int sleepMaxTimeMS = 20;
		double windMinXY = -10;
		double windMaxXY = 10;

		PositionData station1Position = new PositionData(1, 2);
		this.uriMS1 = AbstractComponent.createComponent(
				MeteoStation.class.getCanonicalName(),
				new Object[] { MS1ReceivingInboundPortURI, MS1RegistrationOutboundPortURI, MS1PublishingOutboundPortURI, "", // privileged port URI empty
						RegistrationClass.FREE, 1, 0, station1Position, sleepMinTimeMS, sleepMaxTimeMS, windMinXY, windMaxXY});
		assert this.isDeployedComponent(this.uriMS1);
		this.toggleTracing(this.uriMS1);
	
		PositionData station2Position = new PositionData(3, 2);
		this.uriMS2 = AbstractComponent.createComponent(
				MeteoStation.class.getCanonicalName(),
				new Object[] { MS2ReceivingInboundPortURI, MS2RegistrationOutboundPortURI, MS2PublishingOutboundPortURI, "", // privileged port URI empty
						RegistrationClass.FREE, 1, 0, station2Position, sleepMinTimeMS, sleepMaxTimeMS, windMinXY, windMaxXY});
		assert this.isDeployedComponent(this.uriMS2);
		this.toggleTracing(this.uriMS2);
		
		// Creation of 1 WindTurbine between both MeteoStations
		PositionData windTurbinePosition = new PositionData(2, 2);
		this.uriWindTurbine = AbstractComponent.createComponent(WindTurbine.class.getCanonicalName(),
				new Object[] { WindTurbineReceivingInboundPortURI, WindTurbineRegistrationOutboundPortURI, WindTurbinePublishingOutboundPortURI,
						RegistrationClass.FREE, 1, 0, windTurbinePosition, MeteoAlertI.Level.ORANGE});
		assert this.isDeployedComponent(this.uriWindTurbine);
		this.toggleTracing(this.uriWindTurbine);

		// --------------------------------------------------------------------
		// Deployment done
		// --------------------------------------------------------------------

		super.deploy();
		assert this.deploymentDone();
	}
	
	public static void main(String[] args) {
		try {
			CVM a = new CVM() ;
			
			int duree = 120; // in seconds
			a.startStandardLifeCycle(duree * 1000L) ; // start

			System.exit(0) ;
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
}
