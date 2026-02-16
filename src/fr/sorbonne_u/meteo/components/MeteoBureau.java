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
 * Classe représentant un bureau météorologique émettant des alertes aléatoires de manière périodique.
 * @author Kevin TANG
 * @author David VADIMON
 */
@RequiredInterfaces(required= {RegistrationCI.class,PublishingCI.class})
@OfferedInterfaces(offered= {ReceivingCI.class})
public class MeteoBureau extends Client{
	
	private PositionData bureauPosition;
	
	// Attente entre chaque envoi d'alerte, en secondes
	private int sleepMinTime;
	private int sleepMaxTime;

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
	 * pre	{@code sleepMinTime >= 0 && sleepMaxTime >= sleepMinTime}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param registrationOutboundPortURI   L'URI du port sortant réquérant {@link RegistrationCI}.
	 * @param publishingOutboundPortURI     L'URI du port sortant réquérant {@link PublishingCI}.
	 * @param receivingInboundPortURI       L'URI du port entrant offrant {@link ReceivingCI}.
	 * @param bureauPosition                La position du bureau.
	 * @param sleepMinTime					Le temps minimal d'attente entre chaque nouvel envoi de {@link MeteoAlert}.
	 * @param sleepMaxTime					Le temps maximal d'attente entre chaque nouvel envoi de {@link MeteoAlert}.
	 * @throws Exception                    Si problème lors de l'initialisation ou de la publication des ports.
	 */
	protected MeteoBureau(String registrationOutboundPortURI, String publishingOutboundPortURI, String receivingInboundPortURI, 
			PositionData bureauPosition, int sleepMinTime, int sleepMaxTime) throws Exception {
		super(registrationOutboundPortURI, publishingOutboundPortURI, receivingInboundPortURI);
		assert(bureauPosition != null);
		assert(sleepMinTime >= 0 && sleepMaxTime >= sleepMinTime);
		
		this.bureauPosition = bureauPosition;
		this.sleepMinTime = sleepMinTime;
		this.sleepMaxTime = sleepMaxTime;
		
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
		
		// On envoie périodiquement une alerte sur le 'channel1'
	    
	    //this.scheduleNextSend(); // Pour l'instant ne marche pas
	    
	    // temporaire
	    this.runTask(
		        new AbstractComponent.AbstractTask() {
		            @Override
		            public void run() {
		                try {
		                    ((MeteoBureau) this.getTaskOwner()).sendAlert("channel1");
		                } catch (Throwable e) {
		                    e.printStackTrace();
		                }
		            }
		        }
		    );
	}
	
	// --------------------------------------------------------------------------
	// Envoi d'une alerte touchant une région
	// --------------------------------------------------------------------------
	
	/**
	 * Prévoit d'envoyer une nouvelle alerte au bout d'un laps de temps.<br>
	 * Pour l'instant pas utilisé, sera utilisé quand au moins 1 thread schedulable.
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
	 * Envoi une {@link MeteoAlert} avec attributs aléatoires.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param channel			Le nom du channel sur lequel envoyer l'alerte.
	 * @throws Exception		Si problème lors de la publication.
	 */
	private void sendAlert(String channel) throws Exception {
		assert(channel != null && !channel.isEmpty());	
		
		// temporaire
		int seconds = ThreadLocalRandom.current().nextInt(sleepMinTime, sleepMaxTime); 
		this.logMessage("MeteoBureau sleeps "+seconds+" seconds..."); 
		Thread.sleep(seconds * 1000L);
		
		Message msg = new Message();
		
		ArrayList<RegionI> regions = new ArrayList<>();
		regions.add(randomRegion());
		
		MeteoAlertI.AlertType alert = randomAlert();
		MeteoAlertI.Level level = randomLevel();
		Instant instant = Instant.now(); // Début instantanné
		Duration duration = randomDuration();
		MeteoAlert ma =new MeteoAlert(alert, level, regions, instant, duration);
		
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
		
		// temporaire
	    this.runTask(
		        new AbstractComponent.AbstractTask() {
		            @Override
		            public void run() {
		                try {
		                    ((MeteoBureau) this.getTaskOwner()).sendAlert("channel1");
		                } catch (Throwable e) {
		                    e.printStackTrace();
		                }
		            }
		        }
		    );
		
	}
	
	// --------------------------------------------------------------------------
	// Internal methods
	// --------------------------------------------------------------------------
	
	/**
	 * Renvoit un {@link MeteoAlertI.AlertType} aléatoire.
	 * @return Le type d'une alerte.
	 */
	private MeteoAlertI.AlertType randomAlert(){
		int level = ThreadLocalRandom.current().nextInt(0, 6);
	    switch (level) {
        case 0:
            return MeteoAlertI.AlertType.STORM;
        case 1:
            return MeteoAlertI.AlertType.ICY_STORM;
        case 2:
            return MeteoAlertI.AlertType.SNOW_STORM;
        case 3:
            return MeteoAlertI.AlertType.FLOODING;
        case 4:
            return MeteoAlertI.AlertType.EARTHQUAKE;
        case 5:
            return MeteoAlertI.AlertType.ERUPTION;
        default:
            throw new AssertionError("Impossible");
	    }
    }
	
	/**
	 * Renvoit un {@link MeteoAlertI.Level} aléatoire.
	 * @return Le niveau d'une alerte.
	 */
	private MeteoAlertI.Level randomLevel(){
		int level = ThreadLocalRandom.current().nextInt(0, 5);
	    switch (level) {
        case 0:
            return MeteoAlertI.Level.GREEN;
        case 1:
            return MeteoAlertI.Level.YELLOW;
        case 2:
            return MeteoAlertI.Level.ORANGE;
        case 3:
            return MeteoAlertI.Level.RED;
        case 4:
            return MeteoAlertI.Level.SCARLET;
        default:
            throw new AssertionError("Impossible");
	    }
    }
	
	/**
	 * Renvoit une {@link RegionData} aléatoire, couvrant des zones 3 x 3, dans une carte de coordonnées totales 9 x 9.
	 * @return La région d'une alerte.
	 */
	private RegionData randomRegion(){
		// Zones 3 x 3
		double xMin = ThreadLocalRandom.current().nextInt(0, 6);
		double xMax = xMin + 3;
		double yMin = ThreadLocalRandom.current().nextInt(0, 6);
		double yMax = yMin + 3;
		return new RegionData(xMin, xMax, yMin, yMax);
    }

	/**
	 * Renvoit une {@link Duration} aléatoire d'une alerte, entre 5 et 60 secondes. 
	 * @return La durée d'une alerte.
	 */
	private Duration randomDuration() {
		// Durée entre 5 et 60 secondes
	    int seconds = ThreadLocalRandom.current().nextInt(5, 61);
	    return Duration.ofSeconds(seconds);
	}
}
