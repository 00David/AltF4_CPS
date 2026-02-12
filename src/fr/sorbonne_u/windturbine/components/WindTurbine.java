package fr.sorbonne_u.windturbine.components;


import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;


import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.clients.connectors.ClientPublishingServiceConnector;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI.AlertType;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI.AlertTypeI;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI.LevelI;
import fr.sorbonne_u.cps.meteo.interfaces.PositionI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertyFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.filters.MessageFilter;
import fr.sorbonne_u.filters.property.PropertiesFilter;
import fr.sorbonne_u.filters.property.RegionFilter;
import fr.sorbonne_u.filters.time.GreaterOrEqualTimeFilter;
import fr.sorbonne_u.filters.value.PositionFilter;
import fr.sorbonne_u.meteo.data.MeteoAlert;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.meteo.data.WindData;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;

/**
 * Le composant WindTurbine est un Client du systeme
 * Il recupère les messages envoyé par les stations/bureaux méteologiques et prenne les decisions appropriés pour
 * son orientation ou sa mise en sécurité
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */

@RequiredInterfaces(required= {RegistrationCI.class,PublishingCI.class})
@OfferedInterfaces(offered= {ReceivingCI.class})
public class WindTurbine extends Client {
	
	
	//pour stocker les données des messages de vents
	private ArrayList<WindData> datas;
	
	
	//la position de l'eolienne
	private PositionData pos;
	
	
	//l'orientation de l'eolienne
	private PositionData orientation;
	
	
	private String brokerPublishingPortURI = null;
	
	//niveau d'alerte requis pour reagir (reagis aussi aux niveaux superieurs)
	private LevelI reactionLevel;
	
	
	//pour indiquer que l'eolienne s'est mis en sécurité ou pas (false par défaut)
	private boolean safeMode;
	
	/**
	 * Constructeur
	 */

	protected WindTurbine(String registrationOutboundPortURI, String publishingOutboundPortURI,
			String receivingInboundPortURI,PositionI pos,LevelI reactionLevel) throws Exception {
		super(registrationOutboundPortURI, publishingOutboundPortURI, receivingInboundPortURI);
		
		assert(pos != null);
		assert(reactionLevel != null);
		
		this.pos = (PositionData)pos;
		this.datas = new ArrayList<WindData>();
		this.reactionLevel = reactionLevel;
		this.orientation = new PositionData(0, 0);
		this.safeMode = false;
		
		this.getTracer().setTitle("Eolienne") ;
		this.getTracer().setRelativePosition(1, 0) ;
		
	}
	
	
	
	
	
	
	// comportement eolienne
	public void startWindTurbine() throws Exception {
		if(!this.registrationOutboundPort.registered(receivingInboundPort.getPortURI())) {
			brokerPublishingPortURI  = this.registrationOutboundPort.register(receivingInboundPort.getPortURI(), RegistrationClass.FREE);
			this.doPortConnection(this.publishingOutboundPort.getPortURI(),
					brokerPublishingPortURI,
					ClientPublishingServiceConnector.class.getCanonicalName());
			
			
			//on cree un filtre qui recupere seulement les messages des stations proches de l'eolienne recent
			PropertyFilterI[] prop = new PropertyFilterI[2];
			PositionFilter filterPos = new PositionFilter(this.pos);
			
			prop[0] = new RegionFilter("region", filterPos);
			GreaterOrEqualTimeFilter time = new GreaterOrEqualTimeFilter(Instant.now());
			
					
			//vents
			this.registrationOutboundPort.subscribe(
					receivingInboundPort.getPortURI(), 
					"channel0", 
					new MessageFilter(prop, new PropertiesFilter[0], time));
			
			//alertes
			this.registrationOutboundPort.subscribe(receivingInboundPort.getPortURI(),
					"channel1", new MessageFilter(prop, new PropertiesFilter[0], time));
			
			
			
			
		
		}
	}
		
		
	//////Methodes	
	
	/**
	 * Calcule l'orientation à adopter selon les données récupérés.
	 * <p><strong>Contrat</strong></p>
		<pre>
		pre none
		post none
		</pre>
	 */
	private void calculateNeededDirection() throws Exception{
		double x = 0;
		double y = 0;
		
		for(WindData d : datas) {
			double distance = distance(d.getPosition());
			
			//plus la station est proche plus elle influence l'eolienne
			//0.0001 pour eviter la division par zero
			double weight = 1.0 / (distance + 0.0001);
			 x += d.xComponent() * weight;
			 y += d.yComponent() * weight;
		}
		
		
		//coordonnée du vecteur
		x = x * -1;
		y = y * -1;
				
		
		this.orientation = new PositionData(x, y);
		this.logMessage("WindTurbine has changed orientation : now = " + this.orientation );
			
	
	}

	/**Calcule la distance entre l'eolienne et une station metéo.
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
	
	
	/**
	 * Met l'eolienne en mode sécurité.
	 * <p><strong>Contrat</strong></p>
		<pre>
		pre none
		post safeMode == true
		</pre>
	 */
	private void goInSafeMode() {
		if(!this.safeMode) {
			this.logMessage("WindTurbine going into security mode.");
			this.safeMode = true;
		}
		
	}
	
	/**
	 * Remet l'eolienne en fonctionnement normal
	 * <p><strong>Contrat</strong></p>
		<pre>
		pre none
		post safeMode == false
		</pre>
	 */
	private void goBacktoNormalMode() {
		if(this.safeMode) {
			this.logMessage("WindTurbine returning to normal operation.");
			this.safeMode = false;
		}
		
	}
	
	
	
	
	/**
	 * Reaction de l'eolienne en fonction du seuil de reaction et du niveau d'alerte
	 * <p><strong>Contrat</strong></p>
		<pre>
		pre
		post
		</pre>
	 */
	private void react(LevelI lv) {
		
		MeteoAlertI.Level base = (MeteoAlertI.Level) this.reactionLevel;
		MeteoAlertI.Level alert = (MeteoAlertI.Level) lv;
		
		if(base.ordinal() <= alert.ordinal()) {
			goInSafeMode();		
		}else {
			goBacktoNormalMode();
		}
 	}



	
	
	
	//--------------------------------------------------------------------------
	// Component internal services
	//--------------------------------------------------------------------------
	
	
	@Override
	public Void receiveService(String channel, MessageI message){
		this.logMessage("WindTurbine received message '" + message + "' from the channel : " + channel);
		Serializable payload = message.getPayload();
		
		if(payload instanceof WindData) {
			this.datas.add((WindData)payload);
			try {
				calculateNeededDirection();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}else if(payload instanceof MeteoAlert) {
			MeteoAlertI alert = (MeteoAlertI) payload;
			react(alert.getLevel());
		}
		
		
		return null;
	}
	
	
	@Override
	public Void receiveService(String channel, MessageI[] messages) {
		this.logMessage("WindTurbine received multiple messages from the channel : " + channel);
		for(MessageI mes : messages) {
			Serializable payload = mes.getPayload();
			if(payload instanceof WindData) {
				this.datas.add((WindData)payload);
				try {
					calculateNeededDirection();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}else if(payload instanceof MeteoAlert) {
				MeteoAlertI alert = (MeteoAlertI) payload;
				react(alert.getLevel());
			}
		}
		
		return null;
	}

	
	// --------------------------------------------------------------------------
	// Component life-cycle
	// --------------------------------------------------------------------------




	@Override
	public void execute() throws Exception {
		// TODO Auto-generated method stub
		super.execute();
		this.logMessage("executing WindTurbine component");
		
		if(!this.registrationOutboundPort.registered(receivingInboundPort.getPortURI())) {
			brokerPublishingPortURI = this.registrationOutboundPort.register(receivingInboundPort.getPortURI(), RegistrationClass.FREE);
		}
		
		assert (brokerPublishingPortURI != null);
		
		this.doPortConnection(this.receivingInboundPort.getPortURI(), 
				brokerPublishingPortURI,ClientPublishingServiceConnector.class.getCanonicalName());
		
		this.runTask(new AbstractTask() {
			
			@Override
			public void run() {
				try {
				 
				}catch (Throwable e) {
					e.printStackTrace();
				}
				
			}
		});
	}
	
	

	
	

}


	