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
 * Classe de test représentant un bureau météorologique suivant un scénario prédéfini.
 * 
 * <p>
 * Le scénario testé :
 * <li> - Le bureau météo dort durant les 90 premières secondes du programme.
 * <li> - Il envoie une alerte dangereuse (de niveau RED) au 'channel1', de durée 30 secondes.
 * <li> - Il dort ensuite 30 secondes pendant l'alerte.
 * <li> - Il se réveille et renvoit une alerte de fin de danger (de niveau GREEN).
 * <li> - Il dort ensuite 60 secondes.
 * <li> - Il se réveille et renvoit une alerte restreinte à la zone du bureau, ne touchant pas l'éolienne.
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
	private int sleepSinceStart = 90; // en secondes
	private int durationAlertTime = 30; // en secondes
	private int sleepAfterSafeAlert = 60; // en secondes

	//--------------------------------------------------------------------------
	// Constructor
	//--------------------------------------------------------------------------
	
	/**
	 * Crée un composant bureau météorologique du système de publication/souscription, étant un {@link Client}.
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
	 * @param registrationOutboundPortURI   L'URI du port sortant réquérant {@link RegistrationCI}.
	 * @param publishingOutboundPortURI     L'URI du port sortant réquérant {@link PublishingCI}.
	 * @param receivingInboundPortURI       L'URI du port entrant offrant {@link ReceivingCI}.
	 * @param bureauPosition                La position du bureau.
	 * @throws Exception                    Si problème lors de l'initialisation ou de la publication des ports.
	 */
	protected MeteoBureauTest(String registrationOutboundPortURI, String publishingOutboundPortURI, String receivingInboundPortURI, 
			PositionData bureauPosition) throws Exception {
		super(registrationOutboundPortURI, publishingOutboundPortURI, receivingInboundPortURI);
		assert(bureauPosition != null);
		
		this.bureauPosition = bureauPosition;
		
		this.getTracer().setTitle("Bureau Meteo") ;
		this.getTracer().setRelativePosition(1, 0) ;
	}

	// --------------------------------------------------------------------------
	// Component life-cycle
	// --------------------------------------------------------------------------

	@Override
	public void execute() throws Exception {
		super.execute();
		this.logMessage("executing MeteoBureau component");
		
		// Si l'on est pas enregistré sur le Broker
		if (!this.registrationOutboundPort.registered(receivingInboundPort.getPortURI())) {
			// On s'enregistre et on récupère l'URI du port entrant de publication du Broker
			brokerPublishingPortURI = this.registrationOutboundPort.register(receivingInboundPort.getPortURI(), RegistrationClass.FREE);
		}
		assert (brokerPublishingPortURI != null);
		this.logMessage("MeteoBureau registered");
		
		// On connecte le port de publication
		this.doPortConnection(this.publishingOutboundPort.getPortURI(), 
				brokerPublishingPortURI, 
				ClientPublishingServiceConnector.class.getCanonicalName());
		
		sendAlertDanger();
		sendAlertSafe();
		sendUncaughtAlert();
	}
	
	// --------------------------------------------------------------------------
	// Envoi des alertes
	// --------------------------------------------------------------------------

	/**
	 * Envoi une alerte désignant le début d'une alerte dangereuse.
	 * @throws Exception		Si problème lors de la publication.
	 */
	public void sendAlertDanger() throws Exception {

		// Dort pendant un certain nombre de secondes depuis le début
		// Seulement après lance une alerte dangereuse
		this.logMessage("MeteoBureau sleeps "+sleepSinceStart+" seconds..."); 
		Thread.sleep(sleepSinceStart * 1000L);
		
		Message msg = new Message();
		
		ArrayList<RegionI> regions = new ArrayList<>();
		double x = bureauPosition.getPosX(); double y = bureauPosition.getPosY();
		regions.add(new RegionData(x-1, x+1, y-1, y+1)); // Zone 3 x 3 autour du bureau météorologique
		MeteoAlertI.AlertType alert = MeteoAlertI.AlertType.EARTHQUAKE;
		MeteoAlertI.Level level = MeteoAlertI.Level.RED;
		Instant instant = Instant.now(); // Début instantanné
		Duration duration = Duration.ofSeconds(durationAlertTime);
		MeteoAlert ma =new MeteoAlert(alert, level, regions, instant, duration);
		
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
	 * Envoi une alerte désignant la fin de l'alerte précédante.
	 * @throws Exception		Si problème lors de la publication.
	 */
	public void sendAlertSafe() throws Exception {
		
		// Dort le temps de l'alerte dangereuse
		this.logMessage("MeteoBureau sleeps "+durationAlertTime+" seconds..."); 
		Thread.sleep(durationAlertTime * 1000L);
		
		Message msg = new Message();
		
		ArrayList<RegionI> regions = new ArrayList<>();
		double x = bureauPosition.getPosX(); double y = bureauPosition.getPosY();
		regions.add(new RegionData(x-1, x+1, y-1, y+1)); // Zone 3 x 3
		MeteoAlertI.AlertType alert = MeteoAlertI.AlertType.EARTHQUAKE;
		MeteoAlertI.Level level = MeteoAlertI.Level.GREEN;
		Instant instant = Instant.now(); // Début instantanné
		Duration duration = Duration.ofSeconds(durationAlertTime);
		MeteoAlert ma =new MeteoAlert(alert, level, regions, instant, duration);
		
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
	 * Envoi une alerte non reçue par l'éolienne, après filtrage de la région concernée.
	 * @throws Exception		Si problème lors de la publication.
	 */
	public void sendUncaughtAlert() throws Exception {
		
		// Dort pendant un certain nombre de secondes après le lancement de l'alerte de fin de danger
		this.logMessage("MeteoBureau sleeps "+sleepAfterSafeAlert+" seconds..."); 
		Thread.sleep(sleepAfterSafeAlert * 1000L);
		
		Message msg = new Message();
		
		ArrayList<RegionI> regions = new ArrayList<>();
		double x = bureauPosition.getPosX(); double y = bureauPosition.getPosY();
		regions.add(new RegionData(x, x, y, y)); // Zone 1 x 1 autour du bureau météorologique
		MeteoAlertI.AlertType alert = MeteoAlertI.AlertType.EARTHQUAKE;
		MeteoAlertI.Level level = MeteoAlertI.Level.RED;
		Instant instant = Instant.now(); // Début instantanné
		Duration duration = Duration.ofSeconds(durationAlertTime);
		MeteoAlert ma =new MeteoAlert(alert, level, regions, instant, duration);
		
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