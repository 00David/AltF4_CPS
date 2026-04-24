package fr.sorbonne_u;

import java.util.ArrayList;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractDistributedCVM;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.meteo.components.MeteoBureau;
import fr.sorbonne_u.meteo.components.MeteoStation;
import fr.sorbonne_u.meteo.components.WindTurbine;
import fr.sorbonne_u.meteo.data.PositionData;

public class DistributedCVM extends	AbstractDistributedCVM {
	
	// Ports
	
	// Broker1
	protected final String Broker1GossipInboundPortURI = "gossip-inbound-port-Bro1";
	protected final String Broker1GossipOutboundPortURI = "gossip-outbound-port-Bro1-"; // + "1", +"2", etc..
	
	// Broker2
	protected final String Broker2GossipInboundPortURI = "gossip-inbound-port-Bro2";
	protected final String Broker2GossipOutboundPortURI = "gossip-outbound-port-Bro2-"; // + "1", +"2", etc..
	
	// Broker3
	protected final String Broker3GossipInboundPortURI = "gossip-inbound-port-Bro3";
	protected final String Broker3GossipOutboundPortURI = "gossip-outbound-port-Bro3-"; // + "1", +"2", etc..
	
	// MeteoBureau (with Broker1)
	protected final String MBReceivingInboundPortURI = "inbound-port-client-MB";
	
	// MeteoStation 1 (with Broker1)
	protected final String MS1ReceivingInboundPortURI = "inbound-port-client-MS1";
	
	// MeteoStation 2 (with Broker2)
	protected final String MS2ReceivingInboundPortURI = "inbound-port-client-MS2";
	
	// WindTurbine (with Broker3)
	protected final String WindTurbineReceivingInboundPortURI ="inbound-port-client-WindTurbine";
	
	// JVM URIs
	protected String JVMbroker1 = "broker1";
	protected String JVMbroker2 = "broker2";
	protected String JVMbroker3 = "broker3";
	
	// Component URIs
	protected String uriBroker1;
	protected String uriBroker2;
	protected String uriBroker3;
	protected String uriMB;
	protected String uriMS1;
	protected String uriMS2;
	protected String uriWindTurbine;
	
	// Component general parameters
	protected final int nbThreads = 1; // classic thread pool
	protected final int nbSchedulableThreads = 0;
	protected final RegistrationClass rc = RegistrationClass.FREE;
	
	// Broker parameters, same for each
	protected final int nbDefaultChannels = 2;
	protected final int nbMaxChannelsStd = 2;
	protected final int nbMaxChannelsPrem = 4;
	protected final int nbFreeLoadersThreads = 1;
	protected final int nbUnloadersThreads = 1;
	protected final int nbSendersThreads = 1;
	protected final int nbGossipThreads = 1;
	// Broker parameters, individual
	protected final PositionData broker1Postion = new PositionData(0, 3);
	protected final PositionData broker2Postion = new PositionData(0, 4);
	protected final PositionData broker3Postion = new PositionData(0, 5);
	
	// MeteoBureau parameters
	protected final PositionData bureauPosition = new PositionData(2, 1);
	protected final int sleepMinTimeMB = 20;
	protected final int sleepMaxTimeMB = 30;
	
	// MeteoStation parameters
	protected final PositionData station1Position = new PositionData(1, 2);
	protected final PositionData station2Position = new PositionData(3, 2);
	protected int sleepMinTimeMS = 5;
	protected int sleepMaxTimeMS = 20;
	protected double windMinXY = -10;
	protected double windMaxXY = 10;
	
	// WindTurbine parameters
	protected final PositionData windTurbinePosition = new PositionData(2, 2);
	protected final MeteoAlertI.Level maxWorkingLevel = MeteoAlertI.Level.ORANGE;

	public DistributedCVM(String[] args) throws Exception {
		super(args);
	}

	@Override
	public void	initialise() throws Exception {
		/*AbstractCVM.DEBUG_MODE.add(CVMDebugModes.LIFE_CYCLE);
		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.INTERFACES);
		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.PORTS);
		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.CONNECTING);
		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.CALLING);
		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.EXECUTOR_SERVICES);*/
		super.initialise();
	}

	@Override
	public void	instantiateAndPublish() throws Exception {
		if (thisJVMURI.equals(JVMbroker1)) {
			
			ArrayList<String> broker1OutboundPorts = new ArrayList<>();
			broker1OutboundPorts.add(Broker1GossipOutboundPortURI+"1");
			broker1OutboundPorts.add(Broker1GossipOutboundPortURI+"2");
			
			ArrayList<String> broker1ConnectedTo = new ArrayList<>(); 
			broker1ConnectedTo.add(Broker2GossipInboundPortURI);
			broker1ConnectedTo.add(Broker3GossipInboundPortURI);
			
			// Broker creation
			this.uriBroker1 = AbstractComponent.createComponent(
					Broker.class.getCanonicalName(),
					new Object[] {1, broker1Postion, nbDefaultChannels, nbMaxChannelsStd, nbMaxChannelsPrem, nbThreads, nbSchedulableThreads, 
							nbFreeLoadersThreads, nbUnloadersThreads, nbSendersThreads, nbGossipThreads,
							Broker1GossipInboundPortURI, broker1OutboundPorts, broker1ConnectedTo});
			assert this.isDeployedComponent(this.uriBroker1);
			this.toggleTracing(this.uriBroker1);
			
			// Creation of 1 MeteoBureau
			this.uriMB = AbstractComponent.createComponent(
					MeteoBureau.class.getCanonicalName(),
					new Object[] { MBReceivingInboundPortURI, rc, nbThreads, nbSchedulableThreads, bureauPosition, sleepMinTimeMB, sleepMaxTimeMB});
			assert this.isDeployedComponent(this.uriMB);
			this.toggleTracing(this.uriMB);
			
			// Creation of first MeteoStation
			this.uriMS1 = AbstractComponent.createComponent(
					MeteoStation.class.getCanonicalName(),
					new Object[] { MS1ReceivingInboundPortURI, rc, nbThreads, nbSchedulableThreads, station1Position, sleepMinTimeMS, sleepMaxTimeMS, windMinXY, windMaxXY});
			assert this.isDeployedComponent(this.uriMS1);
			this.toggleTracing(this.uriMS1);

		} else if (thisJVMURI.equals(JVMbroker2)) {

			ArrayList<String> broker2OutboundPorts = new ArrayList<>();
			broker2OutboundPorts.add(Broker2GossipOutboundPortURI+"1");
			broker2OutboundPorts.add(Broker2GossipOutboundPortURI+"2");
			
			ArrayList<String> broker2ConnectedTo = new ArrayList<>(); 
			broker2ConnectedTo.add(Broker1GossipInboundPortURI);
			broker2ConnectedTo.add(Broker3GossipInboundPortURI);
			
			// Broker creation
			this.uriBroker2 = AbstractComponent.createComponent(
					Broker.class.getCanonicalName(),
					new Object[] {2, broker2Postion, nbDefaultChannels, nbMaxChannelsStd, nbMaxChannelsPrem, nbThreads, nbSchedulableThreads, 
							nbFreeLoadersThreads, nbUnloadersThreads, nbSendersThreads, nbGossipThreads,
							Broker2GossipInboundPortURI, broker2OutboundPorts, broker2ConnectedTo});
			assert this.isDeployedComponent(this.uriBroker2);
			this.toggleTracing(this.uriBroker2);
			
			// Creation of second MeteoStation
			this.uriMS2 = AbstractComponent.createComponent(
					MeteoStation.class.getCanonicalName(),
					new Object[] { MS2ReceivingInboundPortURI, rc, nbThreads, nbSchedulableThreads, station2Position, sleepMinTimeMS, sleepMaxTimeMS, windMinXY, windMaxXY});
			assert this.isDeployedComponent(this.uriMS2);
			this.toggleTracing(this.uriMS2);

		} else if (thisJVMURI.equals(JVMbroker3)) {
			
			ArrayList<String> broker3OutboundPorts = new ArrayList<>();
			broker3OutboundPorts.add(Broker3GossipOutboundPortURI+"1");
			broker3OutboundPorts.add(Broker3GossipOutboundPortURI+"2");
			
			ArrayList<String> broker3ConnectedTo = new ArrayList<>(); 
			broker3ConnectedTo.add(Broker1GossipInboundPortURI);
			broker3ConnectedTo.add(Broker2GossipInboundPortURI);
			
			// Broker creation
			this.uriBroker3 = AbstractComponent.createComponent(
					Broker.class.getCanonicalName(),
					new Object[] {3, broker3Postion, nbDefaultChannels, nbMaxChannelsStd, nbMaxChannelsPrem, nbThreads, nbSchedulableThreads, 
							nbFreeLoadersThreads, nbUnloadersThreads, nbSendersThreads, nbGossipThreads,
							Broker3GossipInboundPortURI, broker3OutboundPorts, broker3ConnectedTo});
			assert this.isDeployedComponent(this.uriBroker3);
			this.toggleTracing(this.uriBroker3);
			
			// Creation of 1 WindTurbine, spatially between both MeteoStations
			this.uriWindTurbine = AbstractComponent.createComponent(WindTurbine.class.getCanonicalName(),
					new Object[] { WindTurbineReceivingInboundPortURI, rc, nbThreads, nbSchedulableThreads, windTurbinePosition, maxWorkingLevel});
			assert this.isDeployedComponent(this.uriWindTurbine);
			this.toggleTracing(this.uriWindTurbine);

		} else {
			System.err.println("Unknown JVM URI... " + thisJVMURI);
		}

		super.instantiateAndPublish();
	}

	public static void main(String[] args) {
		try {
			DistributedCVM da  = new DistributedCVM(args);
			int duree = 90; // in seconds
			da.startStandardLifeCycle(duree * 1000L) ; // start
			System.exit(0);
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
}

