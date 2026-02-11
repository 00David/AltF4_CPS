package fr.sorbonne_u;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.meteo.data.WindData;
import fr.sorbonne_u.meteo_bureau.components.MeteoBureauTest;
import fr.sorbonne_u.meteo_station.components.MeteoStation;
import fr.sorbonne_u.meteo_station.components.MeteoStationTest1;
import fr.sorbonne_u.meteo_station.components.MeteoStationTest2;

/**
 * Classe de test permettant de tester le scénario suivant depuis le début de la CVM :
 * <p>
 * <li> - Au bout de 10 secondes, la {@link MeteoStationTest1} envoie une data de vent au 'channel0'.
 * <li> - Au bout de 20 secondes, la {@link MeteoStationTest2} envoie une data de vent au 'channel0'.
 * <li> - Au bout de 30 secondes, le {@link MeteoBureauTest} envoie une alerte dangereuse au 'channel1'.
 * <li> - Au bout de 40 secondes, le {@link MeteoBureauTest} envoie une alerte de fin de danger au 'channel1'.
 * <li> - Au bout de 50 secondes, la {@link MeteoStationTest1} renvoie une data de vent au 'channel0'.
 * </p>
 * 
 * <p>
 * La CVM s'arrête au bout de 80 secondes.
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
	
	// URIs des composants
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
		
		// création du Broker
		this.uriBroker = AbstractComponent.createComponent(
				Broker.class.getCanonicalName(),
				new Object[] {3});
		assert this.isDeployedComponent(this.uriBroker);
		this.toggleTracing(this.uriBroker);

		
		// création d'un bureau météo
		PositionData bureauPosition = new PositionData(2, 1);
		this.uriMB = AbstractComponent.createComponent(
				MeteoBureauTest.class.getCanonicalName(),
				new Object[] { MBRegistrationOutboundPortURI, MBPublishingOutboundPortURI, MBReceivingInboundPortURI,
						bureauPosition});
		assert this.isDeployedComponent(this.uriMB);
		this.toggleTracing(this.uriMB);
		
		// création de 2 stations météo
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
		
		// création d'une éolienne entre les 2 stations météo
		PositionData windTurbinePosition = new PositionData(2, 2);
		//...

		// --------------------------------------------------------------------
		// Deployment done
		// --------------------------------------------------------------------

		super.deploy();
		assert this.deploymentDone();
	}
	
	public static void main(String[] args) {
		try {
			CVMTest a = new CVMTest() ;
			
			int duree = 80; // en secondes
			a.startStandardLifeCycle(duree * 1000L) ; // démarrage

			System.exit(0) ;
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
}
