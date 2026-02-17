package fr.sorbonne_u;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.components.helpers.CVMDebugModes;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI.AlertType;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI.AlertTypeI;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI.LevelI;
import fr.sorbonne_u.cps.meteo.interfaces.PositionI;
import fr.sorbonne_u.meteo.components.MeteoBureau;
import fr.sorbonne_u.meteo.components.MeteoStation;
import fr.sorbonne_u.meteo.components.WindTurbine;
import fr.sorbonne_u.meteo.data.PositionData;

/**
 * Classe principale de lancement du programme.
 * @author Kevin TANG
 * @author David VADIMON
 */
public class CVM extends AbstractCVM {

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
	
	// URIs des composants
	protected String uriBroker;
	protected String uriMB;
	protected String uriMS1;
	protected String uriMS2;
	protected String uriWindTurbine;
	
	public CVM() throws Exception{
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
		
		// création du Broker
		int nbMaxChannelsSTD = 2;
		int nbMaxChannelsPREM = 4;
		this.uriBroker = AbstractComponent.createComponent(
				Broker.class.getCanonicalName(),
				new Object[] {2, nbMaxChannelsSTD, nbMaxChannelsPREM});
		assert this.isDeployedComponent(this.uriBroker);
		this.toggleTracing(this.uriBroker);

		
		// création d'un bureau météo
		PositionData bureauPosition = new PositionData(2, 1);
		int sleepMinTimeMB = 20;
		int sleepMaxTimeMB = 30;
		this.uriMB = AbstractComponent.createComponent(
				MeteoBureau.class.getCanonicalName(),
				new Object[] { MBRegistrationOutboundPortURI, MBPublishingOutboundPortURI, MBReceivingInboundPortURI,
						bureauPosition, sleepMinTimeMB, sleepMaxTimeMB});
		assert this.isDeployedComponent(this.uriMB);
		this.toggleTracing(this.uriMB);
		
		// création de 2 stations météo
		int sleepMinTimeMS = 5;
		int sleepMaxTimeMS = 20;
		double windMinXY = -10;
		double windMaxXY = 10;

		PositionData station1Position = new PositionData(1, 2);
		this.uriMS1 = AbstractComponent.createComponent(
				MeteoStation.class.getCanonicalName(),
				new Object[] { MS1RegistrationOutboundPortURI, MS1PublishingOutboundPortURI, MS1ReceivingInboundPortURI,
						station1Position, sleepMinTimeMS, sleepMaxTimeMS, windMinXY, windMaxXY});
		assert this.isDeployedComponent(this.uriMS1);
		this.toggleTracing(this.uriMS1);
	
		PositionData station2Position = new PositionData(3, 2);
		this.uriMS2 = AbstractComponent.createComponent(
				MeteoStation.class.getCanonicalName(),
				new Object[] { MS2RegistrationOutboundPortURI, MS2PublishingOutboundPortURI, MS2ReceivingInboundPortURI,
						station2Position, sleepMinTimeMS, sleepMaxTimeMS, windMinXY, windMaxXY});
		assert this.isDeployedComponent(this.uriMS2);
		this.toggleTracing(this.uriMS2);
		
		// création d'une éolienne entre les 2 stations météo
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
			CVM a = new CVM() ;
			
			int duree = 120; // en secondes
			a.startStandardLifeCycle(duree * 1000L) ; // démarrage

			System.exit(0) ;
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
}
