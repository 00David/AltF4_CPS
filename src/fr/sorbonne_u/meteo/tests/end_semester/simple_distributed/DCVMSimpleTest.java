package fr.sorbonne_u.meteo.tests.end_semester.simple_distributed;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractDistributedCVM;
import fr.sorbonne_u.components.utils.tests.TestScenario;
import fr.sorbonne_u.components.utils.tests.TestStep;
import fr.sorbonne_u.components.utils.tests.TestStepI;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI;
import fr.sorbonne_u.cps.meteo.interfaces.RegionI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertyFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.TimeFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.filters.MessageFilter;
import fr.sorbonne_u.filters.property.PropertiesFilter;
import fr.sorbonne_u.filters.property.PropertyFilter;
import fr.sorbonne_u.filters.time.GreaterOrEqualTimeFilter;
import fr.sorbonne_u.filters.value.meteo.PositionInRegionsValueFilter;
import fr.sorbonne_u.messages.meteo.AlertMessage;
import fr.sorbonne_u.meteo.components.MeteoBureau;
import fr.sorbonne_u.meteo.components.MeteoStation;
import fr.sorbonne_u.meteo.components.WindTurbine;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.meteo.data.RegionData;
import fr.sorbonne_u.meteo.tests.audit1.CVMSimpleTest;
import fr.sorbonne_u.utils.aclocks.ClocksServer;

/**
 * Test distributed version of {@link CVMSimpleTest}. 
 * 
 * <p>
 * In terms of JVM occupations :
 * <ul>
 * 	<li>There are <strong>3 Brokers</strong>, each in its own JVM and connected to others with the gossip protocol.</li>
 * 	<li>In Broker n°1's JVM : MeteoBureau, MeteoStation n°1 and the clock server.</li>
 * 	<li>In Broker n°2's JVM : MeteoStation n°2.</li>
 * 	<li>In Broker n°3's JVM : WindTurbine.</li>
 * </ul>
 * </p>
 * 
 * The scenario from the start of the DCVM:
 * 
 * <ul>
 * 	<li> - After 5 seconds, <strong>WindTurbine</strong> subscribes to default channels "channel0" and "channel1".
 * 	<li> - After 30 seconds, <strong>MeteoStation n°1</strong> sends wind data to 'channel0'.
 * 	<li> - After 60 seconds, <strong>MeteoStation n°2</strong> sends wind data to 'channel0'.
 * 	<li> - After 90 seconds, <strong>MeteoBureau</strong> sends a dangerous alert to 'channel1'.
 * 	<li> - After 120 seconds, <strong>MeteoBureau</strong> sends an end-of-danger alert to 'channel1'.
 * 	<li> - After 150 seconds, <strong>MeteoStation n°1</strong> sends wind data again to 'channel0'.
 * 	<li> - After 180 seconds, <strong>MeteoBureau</strong> sends a dangerous alert to 'channel1', not affecting the wind turbine region.
 * </ul>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class DCVMSimpleTest extends AbstractDistributedCVM {
	
	// -------------------------------------------------------------------------
	// Test constants and variables
	// -------------------------------------------------------------------------

	public static String CLOCK_URI = "test-clock";
	public static String START_INSTANT = "2026-01-30T09:00:00.00Z";
	public static String END_INSTANT = "2026-01-30T09:03:20.00Z";

	public static long DELAY_TO_START = 1000L;
	public static long END_SLEEP_DURATION = 10000L;
	public static double ACCELERATION_FACTOR = 3.0;
	public static long EXECUTION_DURATION =
		(long) (DELAY_TO_START + 1000L + ((Instant.parse(END_INSTANT).toEpochMilli() - Instant.parse(START_INSTANT).toEpochMilli())
														/ACCELERATION_FACTOR));
	
	// Delay after the start when steps will be executed, in seconds
	public static long[] STEPS = new long[] {5, 30, 60, 90, 120, 150, 180};
	public static final int NBSTEPS = STEPS.length;

	// -------------------------------------------------------------------------
	// Component datas
	// -------------------------------------------------------------------------
	
	// Number of threads
	protected final int brokerNbThreads = 1;
	protected final int brokerNbSchedulableThreads = 0;
	protected final int brokerNbFreeLoadersThreads = 1;
	protected final int brokerNbUnloadersThreads = 1;
	protected final int brokerNbSendersThreads = 1;
	protected final int brokerNbGossipThreads = 1;
	
	protected final int clientNbThreads = 1;
	protected final int clientNbSchedulableThreads = 1;
	
	// Broker1
	protected final String Broker1GossipInboundPortURI = "gossip-inbound-port-Bro1";
	protected final String Broker1GossipOutboundPortURI = "gossip-outbound-port-Bro1-"; // + "1", +"2", etc..
	protected final PositionData broker1Postion = new PositionData(0, 3);
	
	// Broker2
	protected final String Broker2GossipInboundPortURI = "gossip-inbound-port-Bro2";
	protected final String Broker2GossipOutboundPortURI = "gossip-outbound-port-Bro2-"; // + "1", +"2", etc..
	protected final PositionData broker2Postion = new PositionData(0, 4);
	
	// Broker3
	protected final String Broker3GossipInboundPortURI = "gossip-inbound-port-Bro3";
	protected final String Broker3GossipOutboundPortURI = "gossip-outbound-port-Bro3-"; // + "1", +"2", etc..
	protected final PositionData broker3Postion = new PositionData(0, 5);
	
	// MeteoBureau (with Broker1)
	protected final String MBReceivingInboundPortURI = "inbound-port-client-MB";
	protected static final PositionData bureauPosition = new PositionData(2, 1);
	
	// MeteoStation 1 (with Broker1)
	protected final String MS1ReceivingInboundPortURI = "inbound-port-client-MS1";
	protected final PositionData station1Position = new PositionData(1, 2);
	
	// MeteoStation 2 (with Broker2)
	protected final String MS2ReceivingInboundPortURI = "inbound-port-client-MS2";
	protected final PositionData station2Position = new PositionData(3, 2);
	
	// WindTurbine (with Broker3)
	protected final String WindTurbineReceivingInboundPortURI = "inbound-port-client-WindTurbine";
	protected final static PositionData windTurbinePosition = new PositionData(2, 2);
	protected final MeteoAlertI.Level maxWorkingLevelWT = MeteoAlertI.Level.ORANGE; // inclusive
	
    // Creation of a filter only accepting messages after the start of the scenario
    protected final static TimeFilterI timeFilter = new GreaterOrEqualTimeFilter(Instant.parse(START_INSTANT));
    // No special constraint on wind data reception
    protected final static MessageFilterI windTurbineWindFilter = new MessageFilter(new PropertyFilter[0], new PropertiesFilter[0], timeFilter);
    // On alert reception : creation of a filter accepting alerts with targeted regions containing the wind turbine position
    protected final static PropertyFilterI[] propFiltersAlert = {new PropertyFilter(AlertMessage.ALERT_REGIONS_PROPERTY, new PositionInRegionsValueFilter(windTurbinePosition))};
    protected final static MessageFilterI windTurbineAlertFilter = new MessageFilter(propFiltersAlert, new PropertiesFilter[0], timeFilter);
	
	// Component general parameters
	protected final RegistrationClass rc = RegistrationClass.FREE;
	
	// Broker common parameters
	protected final int nbDefaultChannels = 2;
	protected final int nbMaxChannelsStd = 2;
	protected final int nbMaxChannelsPrem = 4;
    
	// JVM URIs
	protected final String JVMbroker1 = "broker1";
	protected final String JVMbroker2 = "broker2";
	protected final String JVMbroker3 = "broker3";
    
	// Component URIs
	protected static String BROKER1_URI = "Broker1-URI";
	protected static String BROKER2_URI = "Broker2-URI";
	protected static String BROKER3_URI = "Broker3-URI";
	protected static String MB_URI = "MeteoBureau-URI";
	protected static String MS1_URI = "MeteoStation1-URI";
	protected static String MS2_URI = "MeteoStation2-URI";
	protected static String WT_URI = "WindTurbine-URI";

	// -------------------------------------------------------------------------
	// Test scenario steps
	// -------------------------------------------------------------------------
	
	/**
	 * Returns the test scenario described in the class documentation.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return != null}
	 * </pre>
	 * 
	 * @return					The test scenario described in the class documentation.
	 */
	public static TestScenario	testScenario() {
		Instant startInstant = Instant.parse(START_INSTANT);
		Instant endInstant = Instant.parse(END_INSTANT);

		// Scenario instants
		Instant[] step_Instants = new Instant[NBSTEPS];
		for (int i=0; i<NBSTEPS; i++) step_Instants[i] = startInstant.plusSeconds(STEPS[i]);
		
		double x; double y;

		// MeteoBureau alert regions
		x = bureauPosition.getPosX(); y = bureauPosition.getPosY(); 
		ArrayList<RegionI> step2_3_regions = new ArrayList<>(); // for steps 2 and 3
		step2_3_regions.add(new RegionData(x-1, x+1, y-1, y+1)); // 3x3 zone around the office
		ArrayList<RegionI> step5_regions = new ArrayList<>(); // for step 5
		step5_regions.add(new RegionData(x, x, y, y)); // zone exactly on the MeteoBureau
		
		// The MeteoBureau component is used for printing logs on the console
		return new TestScenario(
			CLOCK_URI,
			startInstant,
			endInstant,
			new TestStepI[] {
				new TestStep(
						CLOCK_URI,
						MB_URI, // MeteoBureau component is used for printing logs on the console
						step_Instants[0],
						owner -> {
							System.out.println("STEP 0/"+(NBSTEPS-1)+" ("+STEPS[0]+"s) : WindTurbine subscribes to default channels 'channel0' and 'channel1'");
						}),
				// WindTurbine subscribes to default channels "channel0" and "channel1"
				new TestStep(
						CLOCK_URI, 
						WT_URI, 
						step_Instants[0],
						owner -> {
							try {
								((WindTurbine)owner).subscribeToChannel("channel0", windTurbineWindFilter);
								((WindTurbine)owner).subscribeToChannel("channel1", windTurbineAlertFilter);
							} catch (Exception e) {
								System.err.println("Step 0 WT subscriptions failed with exception " + e);
							}
						}),
				
				// MeteoStation1 sends wind data to 'channel0'.
				new TestStep(
						CLOCK_URI,
						MB_URI, // MeteoBureau component is used for printing logs on the console
						step_Instants[1],
						owner -> {
							System.out.println("STEP 1/"+(NBSTEPS-1)+" ("+STEPS[1]+"s) : MeteoStation1 sends wind data to 'channel0'");
						}),
				new TestStep(
					CLOCK_URI,
					MS1_URI,
					step_Instants[1],
					owner -> {
						try {
							((MeteoStation)owner).publishWindData(2.0, 2.0, "channel0", false);
						} catch (Exception e) {
							System.err.println("Step 1 wind data publication failed with exception " + e);
						}
					}),
				
				// MeteoStation2 sends wind data to 'channel0'.
				new TestStep(
						CLOCK_URI,
						MB_URI, // MeteoBureau component is used for printing logs on the console
						step_Instants[2],
						owner -> {
							System.out.println("STEP 2/"+(NBSTEPS-1)+" ("+STEPS[2]+"s) : MeteoStation2 sends wind data to 'channel0'");
						}),
				new TestStep(
					CLOCK_URI,
					MS2_URI,
					step_Instants[2],
					owner -> {
						try {
							((MeteoStation)owner).publishWindData(1.0, 1.0, "channel0", false);
						} catch (Exception e) {
							System.err.println("Step 2 wind data publication failed with exception " + e);
						}
					}),
				
				// MeteoBureau sends a dangerous alert to 'channel1'.
				new TestStep(
						CLOCK_URI,
						MB_URI,
						step_Instants[3],
						owner -> {
							try {
								System.out.println("STEP 3/"+(NBSTEPS-1)+" ("+STEPS[3]+"s) : MeteoBureau sends a dangerous alert to 'channel1'");
								((MeteoBureau)owner).publishAlert(MeteoAlertI.AlertType.EARTHQUAKE, MeteoAlertI.Level.RED, 
										step2_3_regions, step_Instants[3], Duration.ofSeconds((long)((STEPS[4]-STEPS[3])/ACCELERATION_FACTOR)), "channel1");
							} catch (Exception e) {
								System.err.println("Step 3 alert publication failed with exception " + e);
							}
						}),
				
				// MeteoBureau sends an end-of-danger alert to 'channel1'.
				new TestStep(
						CLOCK_URI,
						MB_URI,
						step_Instants[4],
						owner -> {
							try {
								System.out.println("STEP 4/"+(NBSTEPS-1)+" ("+STEPS[4]+"s) : MeteoBureau sends an end-of-danger alert to 'channel1'");
								((MeteoBureau)owner).publishAlert(MeteoAlertI.AlertType.EARTHQUAKE, MeteoAlertI.Level.GREEN, 
										step2_3_regions, step_Instants[3], Duration.ofSeconds((long)((STEPS[4]-STEPS[3])/ACCELERATION_FACTOR)), "channel1");
							} catch (Exception e) {
								System.err.println("Step 4 alert publication failed with exception " + e);
							}
						}),
				
				// MeteoStation1 sends wind data to 'channel0'.
				new TestStep(
						CLOCK_URI,
						MB_URI, // MeteoBureau component is used for printing logs on the console
						step_Instants[5],
						owner -> {
							System.out.println("STEP 5/"+(NBSTEPS-1)+" ("+STEPS[5]+"s) : MeteoStation1 sends wind data to 'channel0'");
						}),
				new TestStep(
						CLOCK_URI,
						MS1_URI,
						step_Instants[5],
						owner -> {
							try {
								((MeteoStation)owner).publishWindData(2.0, 2.0, "channel0", false);
							} catch (Exception e) {
								System.err.println("Step 5 wind data publication failed with exception " + e);
							}
						}),
				
				// MeteoBureau sends a dangerous alert to 'channel1', not affecting the wind turbine region.
				new TestStep(
						CLOCK_URI,
						MB_URI,
						step_Instants[6],
						owner -> {
							try {
								System.out.println("STEP 6/"+(NBSTEPS-1)+" ("+STEPS[6]+"s) :  MeteoBureau sends a dangerous alert to 'channel1', not affecting the wind turbine region");
								((MeteoBureau)owner).publishAlert(MeteoAlertI.AlertType.EARTHQUAKE, MeteoAlertI.Level.RED, 
										step5_regions, step_Instants[6], Duration.ofSeconds((long)((STEPS[4]-STEPS[3])/ACCELERATION_FACTOR)), "channel1");
							} catch (Exception e) {
								System.err.println("Step 6 alert publication failed with exception " + e);
							}
						})
				});
	}
	
	public DCVMSimpleTest(String[] args) throws Exception {
		super(args);
	}
	
	@Override
	public void	instantiateAndPublish() throws Exception {
		
		long current = System.currentTimeMillis();
		long unixEpochStartTimeInNanos = TimeUnit.MILLISECONDS.toNanos(current + DELAY_TO_START);
		
		Instant	startInstant = Instant.parse(START_INSTANT);
		TestScenario testScenario = testScenario();
		
		if (thisJVMURI.equals(JVMbroker1)) {
			
			ArrayList<String> broker1OutboundPorts = new ArrayList<>();
			broker1OutboundPorts.add(Broker1GossipOutboundPortURI+"1");
			broker1OutboundPorts.add(Broker1GossipOutboundPortURI+"2");
			
			ArrayList<String> broker1ConnectedTo = new ArrayList<>(); 
			broker1ConnectedTo.add(Broker2GossipInboundPortURI);
			broker1ConnectedTo.add(Broker3GossipInboundPortURI);
			
			// Broker creation
			BROKER1_URI = AbstractComponent.createComponent(
					Broker.class.getCanonicalName(),
					new Object[] {1, broker1Postion, nbDefaultChannels, nbMaxChannelsStd, nbMaxChannelsPrem, brokerNbThreads, 
							brokerNbSchedulableThreads, brokerNbFreeLoadersThreads, brokerNbUnloadersThreads, brokerNbSendersThreads, 
							brokerNbGossipThreads, Broker1GossipInboundPortURI, broker1OutboundPorts, broker1ConnectedTo});
			assert this.isDeployedComponent(BROKER1_URI);
			this.toggleTracing(BROKER1_URI);
			
			// Creation of 1 MeteoBureau
			MB_URI = AbstractComponent.createComponent(
					MeteoBureau.class.getCanonicalName(),
					new Object[] {1, MB_URI, MBReceivingInboundPortURI, rc, clientNbThreads, clientNbSchedulableThreads, 
							bureauPosition, testScenario});
			assert this.isDeployedComponent(MB_URI);
			this.toggleTracing(MB_URI);
			
			// Creation of first MeteoStation
			MS1_URI = AbstractComponent.createComponent(
					MeteoStation.class.getCanonicalName(),
					new Object[] {1, MS1_URI, MS1ReceivingInboundPortURI, rc, clientNbThreads, clientNbSchedulableThreads, 
							station1Position, testScenario});
			assert this.isDeployedComponent(MS1_URI);
			this.toggleTracing(MS1_URI);
			
			// Clock server creation
			String serv = AbstractComponent.createComponent(
					ClocksServer.class.getCanonicalName(),
					new Object[] {
						CLOCK_URI,
						unixEpochStartTimeInNanos,
						startInstant,
						ACCELERATION_FACTOR
					});
			assert this.isDeployedComponent(serv);

		} else if (thisJVMURI.equals(JVMbroker2)) {

			ArrayList<String> broker2OutboundPorts = new ArrayList<>();
			broker2OutboundPorts.add(Broker2GossipOutboundPortURI+"1");
			broker2OutboundPorts.add(Broker2GossipOutboundPortURI+"2");
			
			ArrayList<String> broker2ConnectedTo = new ArrayList<>(); 
			broker2ConnectedTo.add(Broker1GossipInboundPortURI);
			broker2ConnectedTo.add(Broker3GossipInboundPortURI);
			
			// Broker creation
			BROKER2_URI = AbstractComponent.createComponent(
					Broker.class.getCanonicalName(),
					new Object[] {2, broker2Postion, nbDefaultChannels, nbMaxChannelsStd, nbMaxChannelsPrem, brokerNbThreads, 
							brokerNbSchedulableThreads, brokerNbFreeLoadersThreads, brokerNbUnloadersThreads, brokerNbSendersThreads, 
							brokerNbGossipThreads, Broker2GossipInboundPortURI, broker2OutboundPorts, broker2ConnectedTo});
			assert this.isDeployedComponent(BROKER2_URI);
			this.toggleTracing(BROKER2_URI);
			
			// Creation of second MeteoStation
			MS2_URI = AbstractComponent.createComponent(
					MeteoStation.class.getCanonicalName(),
					new Object[] {2, MS2_URI, MS2ReceivingInboundPortURI, rc, clientNbThreads, clientNbSchedulableThreads, 
							station2Position, testScenario});
			assert this.isDeployedComponent(MS2_URI);
			this.toggleTracing(MS2_URI);

		} else if (thisJVMURI.equals(JVMbroker3)) {
			
			ArrayList<String> broker3OutboundPorts = new ArrayList<>();
			broker3OutboundPorts.add(Broker3GossipOutboundPortURI+"1");
			broker3OutboundPorts.add(Broker3GossipOutboundPortURI+"2");
			
			ArrayList<String> broker3ConnectedTo = new ArrayList<>(); 
			broker3ConnectedTo.add(Broker1GossipInboundPortURI);
			broker3ConnectedTo.add(Broker2GossipInboundPortURI);
			
			// Broker creation
			BROKER3_URI = AbstractComponent.createComponent(
					Broker.class.getCanonicalName(),
					new Object[] {3, broker3Postion, nbDefaultChannels, nbMaxChannelsStd, nbMaxChannelsPrem, brokerNbThreads, 
							brokerNbSchedulableThreads, brokerNbFreeLoadersThreads, brokerNbUnloadersThreads, brokerNbSendersThreads, 
							brokerNbGossipThreads, Broker3GossipInboundPortURI, broker3OutboundPorts, broker3ConnectedTo});
			assert this.isDeployedComponent(BROKER3_URI);
			this.toggleTracing(BROKER3_URI);
			
			// Creation of 1 WindTurbine, spatially between both MeteoStations
			WT_URI = AbstractComponent.createComponent(WindTurbine.class.getCanonicalName(),
					new Object[] {1, WT_URI, WindTurbineReceivingInboundPortURI, rc, clientNbThreads, clientNbSchedulableThreads, 
							windTurbinePosition, maxWorkingLevelWT, false, windTurbineWindFilter, windTurbineAlertFilter, testScenario});
			assert this.isDeployedComponent(WT_URI);
			this.toggleTracing(WT_URI);

		} else {
			System.err.println("Unknown JVM URI... " + thisJVMURI);
		}

		super.instantiateAndPublish();
	}
	
	public static void main(String[] args) {
		try {
			DCVMSimpleTest dcvm = new DCVMSimpleTest(args) ;
			dcvm.startStandardLifeCycle(EXECUTION_DURATION);
			Thread.sleep(END_SLEEP_DURATION);
			System.exit(0) ;
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
}
