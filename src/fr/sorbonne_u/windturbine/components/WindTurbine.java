package fr.sorbonne_u.windturbine.components;


import java.io.Serializable;
import java.util.ArrayList;


import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.clients.connectors.ClientPublishingServiceConnector;
import fr.sorbonne_u.cps.meteo.interfaces.PositionI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertyFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.filters.MessageFilter;
import fr.sorbonne_u.filters.property.PropertiesFilter;
import fr.sorbonne_u.filters.property.RegionFilter;
import fr.sorbonne_u.filters.value.PositionFilter;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.meteo.data.WindData;

/**
 * Le composant WindTurbine est un Client du systeme
 * Il recupère les messages envoyé par les stations/bureaux méteologiques et prenne les decisions appropriés pour
 * son orientation ou sa mise en sécurité
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */


public class WindTurbine extends Client {
	
	private ArrayList<WindData> datas;
	private PositionData pos;
	
	
	private String brokerPublishingPortURI = null;

	protected WindTurbine(String registrationOutboundPortURI, String publishingOutboundPortURI,
			String receivingInboundPortURI,PositionI pos) throws Exception {
		super(registrationOutboundPortURI, publishingOutboundPortURI, receivingInboundPortURI);
		this.pos = (PositionData)pos;
		this.datas = new ArrayList<WindData>();
		
	}
	
	
	/**
	 * Calcule l'orientation à adopter selon les données récupérés
	 * <p><strong>Contrat</strong></p>
		<pre>
		pre none
		post none
		</pre>
	 */
	private PositionI calculateNeededDirection() throws Exception{
		double x = 0;
		double y = 0;
		
		for(WindData d : datas) {
			double distance = distance(d.getPosition());
			double weight = 1.0 / (distance + 0.0001);
			 x += d.xComponent() * weight;
			 y += d.yComponent() * weight;
		}
		
		
		//coordonnée du vecteur
		x = x * -1;
		y = y * -1;
				
		
		return new PositionData(x, y);
			
	
	}

	/**Calcule la distance entre l'eolienne et une station metéo
	 * 
	 * <p><strong>Contrat</strong></p>
		<pre>
		pre stationPos != null && stationPos instanceof PositionI
		post none
		</pre>
		
		@param stationPos la Position de la station
		@throws Excepion
	 */
	private double distance(PositionI stationPos) throws Exception {
		if(stationPos instanceof PositionData) {
			PositionData stationP = (PositionData) stationPos;
			double dx = stationP.getPosX() - this.pos.getPosX();
			double dy = stationP.getPosY() - this.pos.getPosY();
			return Math.sqrt(dx*dx + dy*dy);
	
			
		}else {
			String message = "Impossible";
			throw new IllegalArgumentException(message);
		}
		
		
	}



	// comportement eolienne
	public void estimateWindTurbineOrientation() throws Exception {
		if(!this.registrationOutboundPort.registered(receivingInboundPort.getPortURI())) {
			brokerPublishingPortURI  = this.registrationOutboundPort.register(receivingInboundPort.getPortURI(), RegistrationClass.FREE);
			this.doPortConnection(this.publishingOutboundPort.getPortURI(),
					brokerPublishingPortURI,
					ClientPublishingServiceConnector.class.getCanonicalName());
			
			
			//on cree un filtre qui recupere seulement les messages des stations proches de l'eolienne
			PropertyFilterI[] prop = new PropertyFilterI[1];
			PositionFilter filterPos = new PositionFilter(this.pos);
			prop[0] = new RegionFilter("region", filterPos);
			
					
			this.registrationOutboundPort.subscribe(
					receivingInboundPort.getPortURI(), 
					"Channel0", 
					new MessageFilter(prop, new PropertiesFilter[0], null));
			
			
		
		}
	}
	
	
	//--------------------------------------------------------------------------
	// Component internal services
	//--------------------------------------------------------------------------
	
	
	@Override
	public Void receiveService(String channel, MessageI message){
		this.logMessage("WindTurbine received message '" + message + "' from the channel : " + channel);
		WindData d = (WindData)message.getPayload();
		
		this.datas.add(d);
		
		return null;
	}
	
	
	@Override
	public Void receiveService(String channel, MessageI[] messages) {
		this.logMessage("WindTurbine received multiple messages from the channel : " + channel);
		for(MessageI mes : messages) {
			WindData d = (WindData)mes.getPayload();
			this.datas.add(d);
		}
		
		return null;
	}

}


	