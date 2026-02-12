package fr.sorbonne_u.meteo_station.components;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.clients.connectors.ClientPublishingServiceConnector;
import fr.sorbonne_u.components.AbstractComponent;
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
 * Classe représentant une station météorologique émettant des donnéees de vents aléatoires, pour sa position, de manière périodique.
 * @author Kevin TANG
 * @author David VADIMON
 */
@RequiredInterfaces(required= {RegistrationCI.class,PublishingCI.class})
@OfferedInterfaces(offered= {ReceivingCI.class})
public class MeteoStation extends Client {
	
	private PositionData stationPosition;

	// Attente entre chaque envoi de WindData, en secondes
	private int sleepMinTime;
	private int sleepMaxTime;
	
	// Bornes dans lequelles générér les composantes X et Y des vecteurs de vent 
	private double windMinXY;
	private double windMaxXY;
	
	// --------------------------------------------------------------------------
	// Constructor
	// --------------------------------------------------------------------------

	/**
	 * Crée un composant station météorologique du système de publication/souscription, étant un {@link Client}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code registrationOutboundPortURI != null && !registrationOutboundPortURI.isEmpty()}
	 * pre	{@code publishingOutboundPortURI != null && !publishingOutboundPortURI.isEmpty()}
	 * pre	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
	 * pre	{@code stationPosition != null}
	 * pre	{@code sleepMinTime >= 0 && sleepMaxTime >= sleepMinTime}
	 * pre	{@code windMaxXY >= windMinXY}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param registrationOutboundPortURI   L'URI du port sortant réquérant {@link RegistrationCI}.
	 * @param publishingOutboundPortURI     L'URI du port sortant réquérant {@link PublishingCI}.
	 * @param receivingInboundPortURI       L'URI du port entrant offrant {@link ReceivingCI}.
	 * @param stationPosition               La position de la station.
	 * @param sleepMinTime					Le temps minimal d'attente entre chaque nouvel envoi de {@link WindData}.
	 * @param sleepMaxTime					Le temps maximal d'attente entre chaque nouvel envoi de {@link WindData}.
	 * @param windMinXY						Le X minimal d'un nouveau vecteur de vent {@link WindData}.
	 * @param windMaxXY						Le X maximal d'un nouveau vecteur de vent {@link WindData}.
	 * @throws Exception                    Si problème lors de l'initialisation ou de la publication des ports.
	 */
	protected MeteoStation(String registrationOutboundPortURI, String publishingOutboundPortURI, String receivingInboundPortURI, 
			PositionData stationPosition, int sleepMinTime, int sleepMaxTime, double windMinXY, double windMaxXY) throws Exception {
		super(registrationOutboundPortURI, publishingOutboundPortURI, receivingInboundPortURI);
		assert(stationPosition != null);
		assert(sleepMinTime >= 0 && sleepMaxTime >= sleepMinTime);
		assert(windMaxXY >= windMinXY);

		this.stationPosition = stationPosition;
		this.sleepMinTime = sleepMinTime;
		this.sleepMaxTime = sleepMaxTime;
		this.windMinXY = windMinXY;
		this.windMaxXY = windMaxXY;
		
		this.getTracer().setTitle("Station Meteo");
		this.getTracer().setRelativePosition(2, 0);
	}
	
	// --------------------------------------------------------------------------
	// Component life-cycle
	// --------------------------------------------------------------------------

	@Override
	public void execute() throws Exception {
		super.execute();
		this.logMessage("executing MeteoStation component");
		
		// Si l'on est pas enregistré sur le Broker
		if (!this.registrationOutboundPort.registered(receivingInboundPort.getPortURI())) {
			// On s'enregistre et on récupère l'URI du port entrant de publication du Broker
			brokerPublishingPortURI = this.registrationOutboundPort.register(receivingInboundPort.getPortURI(), RegistrationClass.FREE);
		}
		assert (brokerPublishingPortURI != null);
		
		// On connecte le port de publication
		this.doPortConnection(this.publishingOutboundPort.getPortURI(), 
				brokerPublishingPortURI, 
				ClientPublishingServiceConnector.class.getCanonicalName());
		
		// On envoie périodiquement une WindData sur le 'channel0'
		
		//this.scheduleNextSend(); // Pour l'instant ne marche pas
		
	    // temporaire
	    this.runTask(
		        new AbstractComponent.AbstractTask() {
		            @Override
		            public void run() {
		                try {
		                    ((MeteoStation) this.getTaskOwner()).sendWindData("channel0");
		                } catch (Throwable e) {
		                    e.printStackTrace();
		                }
		            }
		        }
		    );
	}
	
	// --------------------------------------------------------------------------
	// Envoi d'une WindData
	// --------------------------------------------------------------------------
	
	/**
	 * Prévoit d'envoyer une nouvelle data de vent au bout d'un laps de temps.<br>
	 * Pour l'instant pas utilisé, sera utilisé quand au moins 1 thread schedulable.
	 */
	private void scheduleNextSend() {
	    int seconds = ThreadLocalRandom.current().nextInt(sleepMinTime, sleepMaxTime);

	    this.scheduleTask(
	        new AbstractComponent.AbstractTask() {
	            @Override
	            public void run() {
	                try {
	                    ((MeteoStation) this.getTaskOwner()).sendWindData("channel0");
	                    ((MeteoStation) this.getTaskOwner()).scheduleNextSend();
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
	 * Envoi un {@link WindData} avec des composantes X et Y tirées dans [{@code windMinXY}; {@code windMaxXY}].
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code channel != null && !channel.isEmpty()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param channel			Le nom du channel sur lequel envoyer la data.
	 * @throws Exception		Si problème lors de la publication.
	 */
	private void sendWindData(String channel) throws Exception {
		assert(channel != null && !channel.isEmpty());
		
		// temporaire
		int seconds = ThreadLocalRandom.current().nextInt(sleepMinTime, sleepMaxTime); 
		this.logMessage("MeteoStation sleeps "+seconds+" seconds..."); 
		Thread.sleep(seconds * 1000L);
		
		Message msg = new Message();
		// X et Y du vecteur du vent
		double xWind = ThreadLocalRandom.current().nextDouble(windMinXY, windMaxXY);
		double yWind = ThreadLocalRandom.current().nextDouble(windMinXY, windMaxXY);
		WindData wd = new WindData(stationPosition, xWind, yWind);
		
		msg.setPayload(wd);
		msg.putProperty("position", stationPosition);
		msg.putProperty("xWind", xWind);
		msg.putProperty("yWind", yWind);
		
		this.logMessage("------------------------------");
		this.logMessage("MeteoStation sending wind data "+wd);
		this.logMessage("------------------------------");
		
		this.publishingOutboundPort.publish(receivingInboundPort.getPortURI(), channel, msg);
		
		// temporaire
	    this.runTask(
		        new AbstractComponent.AbstractTask() {
		            @Override
		            public void run() {
		                try {
		                    ((MeteoStation) this.getTaskOwner()).sendWindData("channel0");
		                } catch (Throwable e) {
		                    e.printStackTrace();
		                }
		            }
		        }
		    );
	}
}
