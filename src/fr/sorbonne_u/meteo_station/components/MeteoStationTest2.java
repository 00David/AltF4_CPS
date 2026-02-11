package fr.sorbonne_u.meteo_station.components;

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
 * Classe représentant une station météorologique émettant des donnéees de vents, suivant un scénario prédéfini.
 * 
 * <p>
 * <li> - Le composant va simplement attendre 20 secondes après son lancement, et va envoyer une {@link WindData} au 'channel0'.
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
	private int sleepSinceStart = 20; // en secondes
	
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
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param registrationOutboundPortURI   L'URI du port sortant réquérant {@link RegistrationCI}.
	 * @param publishingOutboundPortURI     L'URI du port sortant réquérant {@link PublishingCI}.
	 * @param receivingInboundPortURI       L'URI du port entrant offrant {@link ReceivingCI}.
	 * @param stationPosition               La position de la station.
	 * @throws Exception                    Si problème lors de l'initialisation ou de la publication des ports.
	 */
	protected MeteoStationTest2(String registrationOutboundPortURI, String publishingOutboundPortURI, String receivingInboundPortURI, 
			PositionData stationPosition) throws Exception {
		super(registrationOutboundPortURI, publishingOutboundPortURI, receivingInboundPortURI);
		assert(stationPosition != null);

		this.stationPosition = stationPosition;
		
		this.getTracer().setTitle("Station Meteo2");
		this.getTracer().setRelativePosition(3, 0);
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
		
		// On prépare l'envoi de la WindData
	    this.runTask(
		        new AbstractComponent.AbstractTask() {
		            @Override
		            public void run() {
		                try {
		                    ((MeteoStationTest2) this.getTaskOwner()).sendWindData();
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
	 * Envoi un {@link WindData} prédéfini au bout de {@code sleepSinceStart} secondes.
	 * @throws Exception		Si problème lors de la publication.
	 */
	private void sendWindData() throws Exception {
		
		this.logMessage("MeteoStation sleeps "+sleepSinceStart+" seconds..."); 
		Thread.sleep(sleepSinceStart * 1000L);
		
		Message msg = new Message();
		// X et Y du vecteur du vent
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
