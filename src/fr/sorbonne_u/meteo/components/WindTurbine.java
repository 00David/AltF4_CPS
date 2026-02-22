package fr.sorbonne_u.meteo.components;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.stream.Stream;

import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.components.annotations.OfferedInterfaces;
import fr.sorbonne_u.components.annotations.RequiredInterfaces;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI.LevelI;
import fr.sorbonne_u.cps.meteo.interfaces.PositionI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertyFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.filters.MessageFilter;
import fr.sorbonne_u.filters.property.PropertiesFilter;
import fr.sorbonne_u.filters.property.PropertyFilter;
import fr.sorbonne_u.filters.time.GreaterOrEqualTimeFilter;
import fr.sorbonne_u.filters.value.PositionInRegionsValueFilter;
import fr.sorbonne_u.meteo.data.MeteoAlert;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.meteo.data.WindData;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.TimeFilterI;

/**
 * Le composant WindTurbine est un {@link Client} du système.
 * 
 * <p>
 * Il récupère les messages envoyés par les stations/bureaux méteorologiques et prend 
 * les decisions appropriées pour son orientation ou sa mise en sécurité.
 * </p>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
@RequiredInterfaces(required = { RegistrationCI.class, PublishingCI.class })
@OfferedInterfaces(offered = { ReceivingCI.class })
public class WindTurbine extends Client {

	// pour stocker les données des messages de vents
	private ArrayList<WindData> datas;

	// la position de l'eolienne
	private PositionData pos;

	// l'orientation de l'eolienne
	private PositionData orientation;

	private String brokerPublishingPortURI = null;

	// Le niveau d'alerte maximal (inclus) pour lequel l'éolienne fonctionne. 
	// Pour un niveau supérieur reçu, l'éolienne se met en sécurité.
	private LevelI maxWorkingLevel;

	// pour indiquer que l'eolienne s'est mis en sécurité ou pas (false par défaut)
	private boolean safeMode;

	//--------------------------------------------------------------------------
	// Constructor
	//--------------------------------------------------------------------------
	
	/**
	 * Crée un composant éolienne du système de publication/souscription, étant un {@link Client}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code registrationOutboundPortURI != null && !registrationOutboundPortURI.isEmpty()}
	 * pre	{@code publishingOutboundPortURI != null && !publishingOutboundPortURI.isEmpty()}
	 * pre	{@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
	 * pre	{@code pos != null}
	 * pre	{@code maxWorkingLevel != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param registrationOutboundPortURI   L'URI du port sortant réquérant {@link RegistrationCI}.
	 * @param publishingOutboundPortURI     L'URI du port sortant réquérant {@link PublishingCI}.
	 * @param receivingInboundPortURI       L'URI du port entrant offrant {@link ReceivingCI}.
	 * @param pos                       	La position de l'éolienne.
	 * @param maxWorkingLevel				Le niveau d'alerte maximal (inclus) pour lequel l'éolienne fonctionne.
	 * @throws Exception                    Si problème lors de l'initialisation ou de la publication des ports.
	 */
	protected WindTurbine(String registrationOutboundPortURI, String publishingOutboundPortURI,
			String receivingInboundPortURI, PositionI pos,LevelI maxWorkingLevel) throws Exception {
		super(registrationOutboundPortURI, publishingOutboundPortURI, receivingInboundPortURI);
		assert (pos != null);
		assert (maxWorkingLevel != null);

		this.pos = (PositionData) pos;
		this.datas = new ArrayList<WindData>();
		this.orientation = new PositionData(0, 0);
		this.safeMode = false;
		this.maxWorkingLevel = maxWorkingLevel;

		this.getTracer().setTitle("Eolienne");
		this.getTracer().setRelativePosition(0, 0);

	}

	// --------------------------------------------------------------------------
	// Component life-cycle
	// --------------------------------------------------------------------------

	@Override
	public void execute() throws Exception {

		super.execute();
		this.logMessage("executing WindTurbine component");

		if (!this.registrationOutboundPort.registered(receivingInboundPort.getPortURI())) {
			brokerPublishingPortURI = this.registrationOutboundPort.register(receivingInboundPort.getPortURI(), RegistrationClass.FREE);
		}

		assert (brokerPublishingPortURI != null);
		this.logMessage("WindTurbine registered");

		startWindTurbine();
	}

	/**
	 * Enregistre l'éolienne sur les channels permettant de recevoir les données des vents et des alertes.
	 * 
	 * <p>
	 * <li> Ne prends en compte que les messages crées après l'enregistrement de l'éolienne.
	 * <li> Pour les alertes, ne prends en compte que les alertes dont une des régions touchées contient la position de l'éolienne.
	 * </p>
	 * @throws Exception Si problème lors de la connexion des ports.
	 */
	public void startWindTurbine() throws Exception {
		
		// on crée un filtre ne récupérant que les messages suivants l'abonnement de l'éolienne
		TimeFilterI timeFilter = new GreaterOrEqualTimeFilter(Instant.now());

		// vents
		this.registrationOutboundPort.subscribe(receivingInboundPort.getPortURI(), "channel0",
				new MessageFilter(new PropertyFilter[0], new PropertiesFilter[0], timeFilter));

		this.logMessage("WindTurbine subscribed to channel0");

		// on crée un filtre ne récupérant que les alertes des bureaux météo touchant la région de la position de l'éolienne
		PropertyFilterI[] propFiltersAlert = new PropertyFilterI[1];
		propFiltersAlert[0] = new PropertyFilter("regions", new PositionInRegionsValueFilter(this.pos));
		
		// alertes
		this.registrationOutboundPort.subscribe(receivingInboundPort.getPortURI(), "channel1",
				new MessageFilter(propFiltersAlert, new PropertiesFilter[0], timeFilter));

		this.logMessage("WindTurbine subscribed to channel1");

	}
	
	// --------------------------------------------------------------------------
	// Component internal services
	// --------------------------------------------------------------------------

	@Override
	public Void receiveService(String channel, MessageI message) {
		// Vérification des préconditions
		assert(channel != null);
		assert(!channel.isEmpty());
		assert(message != null);
		
		this.logMessage("------------------------------");
		this.logMessage("WindTurbine received message '" + message + "' from the channel : " + channel);
		Serializable payload = message.getPayload();

		// Traitement sur réception d'une donnée de vent
		if (payload instanceof WindData && !safeMode) {
			this.datas.add((WindData) payload);
			calculateNeededDirection();
			
		// Traitement sur réception d'une alerte météo
		} else if (payload instanceof MeteoAlert) {
			MeteoAlertI alert = (MeteoAlertI) payload;
			react(alert.getLevel());
			
		// Traitement (aucun) si réception d'une donnée de vent pendant le safeMode
		} else {
			this.logMessage("------------------------------");
		}

		return null;
	}

	@Override
	public Void receiveService(String channel, MessageI[] messages) {
		// Vérification des préconditions
		assert(channel != null);
		assert(!channel.isEmpty());
		assert(messages != null);
		assert(messages.length > 0);
		assert(Stream.of(messages).allMatch(m -> m != null));
		
		this.logMessage("------------------------------");
		this.logMessage("WindTurbine received multiple messages from the channel : " + channel);
		for (MessageI mes : messages) {
			Serializable payload = mes.getPayload();
			
			// Traitement sur réception d'une donnée de vent
			if (payload instanceof WindData && !safeMode) {
				this.datas.add((WindData) payload);
				calculateNeededDirection();
			
			// Traitement sur réception d'une alerte météo
			} else if (payload instanceof MeteoAlert) {
				MeteoAlertI alert = (MeteoAlertI) payload;
				react(alert.getLevel());
				
			// Traitement (aucun) si réception d'une donnée de vent pendant le safeMode
			} else {
				this.logMessage("------------------------------");
			}
		}

		return null;
	}

	// --------------------------------------------------------------------------
	// Component internal methods
	// --------------------------------------------------------------------------

	/**
	 * Calcule l'orientation à adopter selon les données récupérés.
	 */
	private void calculateNeededDirection() {
		double x = 0;
		double y = 0;

		for (WindData d : datas) {
			double distance = distance(d.getPosition());
			double weight = 0;
			
			if(distance == 0) {
				weight = 1.0;
			}else {
				weight = 1.0 / distance;
			}
			
			x += d.xComponent() * weight;
			y += d.yComponent() * weight;
		}

		// coordonnées du vecteur
		x = x * -1;
		y = y * -1;

		this.orientation = new PositionData(x, y);
		this.logMessage("WindTurbine has changed orientation : now = " + this.orientation);
		this.logMessage("------------------------------");
	}
	
	

	/**
	 * Calcule la distance entre l'eolienne et une station metéo.
	 * 
	 * <p><strong>Contrat</strong></p>
	 * <pre>
	 * pre stationPos != null && stationPos instanceof PositionData
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param stationPos 				La Position de la station météo.
	 */
	private double distance(PositionI stationPos) {
		// Vérification des préconditions
		assert(stationPos != null);
		assert(stationPos instanceof PositionData);
		
		PositionData stationP = (PositionData) stationPos;
		double dx = stationP.getPosX() - this.pos.getPosX();
		double dy = stationP.getPosY() - this.pos.getPosY();
		return Math.sqrt(dx * dx + dy * dy);
	}
	
	/**
	 * Réaction de l'éolienne en fonction du seuil de fonctionnement et du niveau d'alerte reçu.
	 * @param level Le niveau de l'alerte reçu.
	 */
	private void react(LevelI level) {

		MeteoAlertI.Level maxLevel = (MeteoAlertI.Level) this.maxWorkingLevel;
		MeteoAlertI.Level alertLevel = (MeteoAlertI.Level) level;

		if (alertLevel.ordinal() > maxLevel.ordinal() && !safeMode) {
			goInSafeMode();
			this.datas.clear(); // On efface les datas de vents précédants
		} else if (alertLevel.ordinal() <= maxLevel.ordinal() && safeMode) {
			goBacktoNormalMode();
		
		// On ne change rien, si l'on a :
		// - reçu une alerte au dessus du niveau maximal de fonctionnement, alors que l'on est déjà en safeMode
		// - reçu une alerte permettant de fonctionner, alors que l'on est déjà en fonctionnement normal (safeMode == false)
		} else {
			this.logMessage("------------------------------");
		}
	}
	
	/**
	 * Met l'eolienne en mode sécurité.
	 * 
	 * <p><strong>Contrat</strong></p>
	 * <pre>
	 * pre {@code true}	// no precondition.
	 * post {@code safemode == true}
	 * </pre>
	 */
	private void goInSafeMode() {
		if (!this.safeMode) {
			this.logMessage("WindTurbine going into security mode.");
			this.logMessage("------------------------------");
			this.safeMode = true;
		}
		assert(this.safeMode == true);
	}

	/**
	 * Remet l'eolienne en fonctionnement normal
	 * 
	 * <p><strong>Contrat</strong></p>
	 * <pre>
	 * pre {@code true}	// no precondition.
	 * post {@code safemode == false}
	 * </pre>
	 */
	private void goBacktoNormalMode() {
		if (this.safeMode) {
			this.logMessage("WindTurbine returning to normal operation.");
			this.logMessage("------------------------------");
			this.safeMode = false;
		}
		assert(this.safeMode == false);
	}

}
