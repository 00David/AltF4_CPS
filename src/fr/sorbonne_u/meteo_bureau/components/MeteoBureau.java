package fr.sorbonne_u.meteo_bureau.components;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.clients.connectors.ClientPublishingServiceConnector;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI;
import fr.sorbonne_u.cps.meteo.interfaces.PositionI;
import fr.sorbonne_u.cps.meteo.interfaces.RegionI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.messages.Message;
import fr.sorbonne_u.meteo.data.MeteoAlert;
import fr.sorbonne_u.meteo.data.RegionData;

/**
 * @author Kevin TANG
 * @author David VADIMON
 */
public class MeteoBureau extends Client{
	
	private PositionI bureauPosition;

	//--------------------------------------------------------------------------
	// Constructor
	//--------------------------------------------------------------------------
	
	protected MeteoBureau(String registrationOutboundPortURI, String publishingOutboundPortURI,
			String receivingInboundPortURI, PositionI bureauPosition) throws Exception {
		super(registrationOutboundPortURI, publishingOutboundPortURI, receivingInboundPortURI);
		
		this.bureauPosition = bureauPosition;
		
		this.getTracer().setTitle("Bureau Meteo") ;
		this.getTracer().setRelativePosition(2, 1) ;
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
		
		// On connecte le port de publication
		this.doPortConnection(this.publishingOutboundPort.getPortURI(), 
				brokerPublishingPortURI, 
				ClientPublishingServiceConnector.class.getCanonicalName());

		// On envoie périodiquement une alerte sur le 'channel0'
		while (true) {
			
			// On sleep entre 1s et 10s
			int seconds = ThreadLocalRandom.current().nextInt(1, 11);
			Thread.sleep(seconds * 1000L);

			// On crée la tâche
			this.runTask(new AbstractComponent.AbstractTask() {
				@Override
				public void run() {
					try {
						((MeteoBureau) this.getTaskOwner()).sendAlert();
					} catch (Throwable e) {
						e.printStackTrace();
					}
				}
			});
		
		}
	}
	
	// --------------------------------------------------------------------------
	// Envoi d'une alerte touchant une région
	// --------------------------------------------------------------------------

	public void sendAlert() throws Exception {
			
		Message msg = new Message();
		ArrayList<RegionI> regions = new ArrayList<>();
		RegionData r = (RegionData)chooseRandomRegion();
		regions.add(r);
		double[] rLimits = r.getLimits();
		
		msg.setPayload(new MeteoAlert(
				chooseRandomAlert(),
				chooseRandomLevel(),
				regions,
				Instant.now(), // Début instantanné
				randomDuration()
				));
			
		this.logMessage("MeteoBureau sending an alert to the region : X["+rLimits[0]+":"+rLimits[1]+"], Y["+rLimits[2]+":"+rLimits[3]+"]");
		this.publishingOutboundPort.publish(receivingInboundPort.getPortURI(), "channel0", msg);
	}
	
	private MeteoAlertI.AlertType chooseRandomAlert(){
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
	
	private MeteoAlertI.Level chooseRandomLevel(){
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
	
	private RegionI chooseRandomRegion(){
		// Zones 3 x 3
		double xMin = ThreadLocalRandom.current().nextInt(0, 7);
		double xMax = xMin + 3;
		double yMin = ThreadLocalRandom.current().nextInt(0, 6);
		double yMax = yMin + 3;
		return new RegionData(xMin, xMax, yMin, yMax);
    }

	private Duration randomDuration() {
		// Durée entre 5 et 60 secondes
	    int seconds = ThreadLocalRandom.current().nextInt(5, 61);
	    return Duration.ofSeconds(seconds);
	}
}
