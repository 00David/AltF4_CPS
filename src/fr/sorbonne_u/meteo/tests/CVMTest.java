package fr.sorbonne_u.meteo.tests;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.components.utils.tests.TestScenario;
import fr.sorbonne_u.components.utils.tests.TestStep;
import fr.sorbonne_u.components.utils.tests.TestStepI;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI;
import fr.sorbonne_u.cps.meteo.interfaces.RegionI;
import fr.sorbonne_u.exceptions.VerboseException;
import fr.sorbonne_u.meteo.components.WindTurbine;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.meteo.data.RegionData;
import fr.sorbonne_u.utils.aclocks.ClocksServer;

/**
 * Test class to simulate the following scenario from the start of the CVM:
 * <p>
 * <li> - After 30 seconds, {@link MeteoStationTest} n°1 sends wind data to 'channel0'.
 * <li> - After 60 seconds, {@link MeteoStationTest} n°2 sends wind data to 'channel0'.
 * <li> - After 90 seconds, {@link MeteoBureauTest} sends a dangerous alert to 'channel1'.
 * <li> - After 120 seconds, {@link MeteoBureauTest} sends an end-of-danger alert to 'channel1'.
 * <li> - After 150 seconds, {@link MeteoStationTest} n°1 sends wind data again to 'channel0'.
 * <li> - After 180 seconds, {@link MeteoBureauTest} sends a dangerous alert to 'channel1', not affecting the wind turbine region.
 * </p>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class CVMTest extends AbstractCVM {
	
	// -------------------------------------------------------------------------
	// Test constants and variables
	// -------------------------------------------------------------------------

	public static String CLOCK_URI = "test-clock";
	public static String START_INSTANT = "2026-01-30T09:00:00.00Z";
	public static String END_INSTANT = "2026-01-30T09:03:20.00Z";

	public static long DELAY_TO_START = 1000L;
	public static long END_SLEEP_DURATION = 10000L;
	public static double ACCELERATION_FACTOR = 1.0;
	public static long EXECUTION_DURATION =
		(long) (DELAY_TO_START + 1000L + ((Instant.parse(END_INSTANT).toEpochMilli() - Instant.parse(START_INSTANT).toEpochMilli())
														/ACCELERATION_FACTOR));
	
	// Delay after the start when the step will be executed, in seconds
	public static long STEP0 = 30; // MeteoStationTest n°1 sends wind data to 'channel0'.
	public static long STEP1 = 60; // MeteoStationTest n°2 sends wind data to 'channel0'.
	public static long STEP2 = 90; // MeteoBureauTest sends a dangerous alert to 'channel1'.
	public static long STEP3 = 120; // MeteoBureauTest sends an end-of-danger alert to 'channel1'.
	public static long STEP4 = 150; // MeteoStationTest n°1 sends wind data to 'channel0'.
	public static long STEP5 = 180; // MeteoBureauTest sends a dangerous alert to 'channel1', not affecting the wind turbine region.

	// -------------------------------------------------------------------------
	// Component datas
	// -------------------------------------------------------------------------
	
	// MeteoBureau
	protected final String MBRegistrationOutboundPortURI = "registration-outbound-port-MB";
	protected final String MBPublishingOutboundPortURI = "publishing-outbound-port-MB";
	protected final String MBReceivingInboundPortURI = "inbound-port-client-MB";
	protected static final PositionData bureauPosition = new PositionData(2, 1);
	
	// MeteoStation 1
	protected final String MS1RegistrationOutboundPortURI = "registration-outbound-port-MS1";
	protected final String MS1PublishingOutboundPortURI = "publishing-outbound-port-MS1";
	protected final String MS1ReceivingInboundPortURI = "inbound-port-client-MS1";
	protected static final PositionData station1Position = new PositionData(1, 2);
	
	// MeteoStation 2
	protected final String MS2RegistrationOutboundPortURI = "registration-outbound-port-MS2";
	protected final String MS2PublishingOutboundPortURI = "publishing-outbound-port-MS2";
	protected final String MS2ReceivingInboundPortURI = "inbound-port-client-MS2";
	protected static final PositionData station2Position = new PositionData(3, 2);
	
	// WindTurbine
	protected final String WindTurbineRegistrationOutboundPortURI = "registration-outbound-port-WindTurbine";
	protected final String WindTurbinePublishingOutboundPortURI = "publishing-outbound-port-WindTurbine";
	protected final String WindTurbineReceivingInboundPortURI = "inbound-port-client-WindTurbine";
	protected static final PositionData windTurbinePosition = new PositionData(2, 2);
	protected static final MeteoAlertI.Level maxWorkingLevelWT = MeteoAlertI.Level.ORANGE; // inclusive
	
	// Component URIs for tests (without the Broker)
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
		
		Instant step0_Instant = startInstant.plusSeconds(STEP0);
		Instant step1_Instant = startInstant.plusSeconds(STEP1);
		Instant step2_Instant = startInstant.plusSeconds(STEP2);
		Instant step3_Instant = startInstant.plusSeconds(STEP3);
		Instant step4_Instant = startInstant.plusSeconds(STEP4);
		Instant step5_Instant = startInstant.plusSeconds(STEP5);
		
		double x; double y;

		// MeteoBureau alert regions
		x = bureauPosition.getPosX(); y = bureauPosition.getPosY(); 
		ArrayList<RegionI> step2_3_regions = new ArrayList<>(); // for steps 2 and 3
		step2_3_regions.add(new RegionData(x-1, x+1, y-1, y+1)); // 3x3 zone around the office
		ArrayList<RegionI> step5_regions = new ArrayList<>(); // for step 5
		step5_regions.add(new RegionData(x, x, y, y)); // zone exactly on the MeteoBureau
		
		return new TestScenario(
			CLOCK_URI,
			startInstant,
			endInstant,
			new TestStepI[] {
				// MeteoStation1 sends wind data to 'channel0'.
				new TestStep(
					CLOCK_URI,
					MS1_URI,
					step0_Instant,
					owner -> {
						try {
							((MeteoStationTest)owner).publishWindData(2.0, 2.0, "channel0");
						} catch (Exception e) {
							System.err.println("Step 0 wind data publication failed with exception " + e);
						}
					}),
				// MeteoStation2 sends wind data to 'channel0'.
				new TestStep(
					CLOCK_URI,
					MS2_URI,
					step1_Instant,
					owner -> {
						try {
							((MeteoStationTest)owner).publishWindData(1.0, 1.0, "channel0");
						} catch (Exception e) {
							System.err.println("Step 1 wind data publication failed with exception " + e);
						}
					}),
				// MeteoBureau sends a dangerous alert to 'channel1'.
				new TestStep(
						CLOCK_URI,
						MB_URI,
						step2_Instant,
						owner -> {
							try {
								((MeteoBureauTest)owner).publishAlert(MeteoAlertI.AlertType.EARTHQUAKE, MeteoAlertI.Level.RED, 
										step2_3_regions, step2_Instant, Duration.ofSeconds(STEP3-STEP2), "channel1");
							} catch (Exception e) {
								System.err.println("Step 2 alert publication failed with exception " + e);
							}
						}),
				// MeteoBureau sends an end-of-danger alert to 'channel1'.
				new TestStep(
						CLOCK_URI,
						MB_URI,
						step3_Instant,
						owner -> {
							try {
								((MeteoBureauTest)owner).publishAlert(MeteoAlertI.AlertType.EARTHQUAKE, MeteoAlertI.Level.GREEN, 
										step2_3_regions, step3_Instant, Duration.ofSeconds(30), "channel1");
							} catch (Exception e) {
								System.err.println("Step 3 alert publication failed with exception " + e);
							}
						}),
				// MeteoStation1 sends wind data to 'channel0'.
				new TestStep(
					CLOCK_URI,
					MS1_URI,
					step4_Instant,
					owner -> {
						try {
							((MeteoStationTest)owner).publishWindData(2.0, 2.0, "channel0");
						} catch (Exception e) {
							System.err.println("Step 4 wind data publication failed with exception " + e);
						}
					}),
				// MeteoBureau sends a dangerous alert to 'channel1', not affecting the wind turbine region.
				new TestStep(
						CLOCK_URI,
						MB_URI,
						step5_Instant,
						owner -> {
							try {
								((MeteoBureauTest)owner).publishAlert(MeteoAlertI.AlertType.EARTHQUAKE, MeteoAlertI.Level.RED, 
										step5_regions, step5_Instant, Duration.ofSeconds(30), "channel1");
							} catch (Exception e) {
								System.err.println("Step 5 alert publication failed with exception " + e);
							}
						})
				});
	}
	
	public CVMTest() throws Exception{
		super();
	}
	
	@Override
	public void	deploy() throws Exception{
		assert	!this.deploymentDone() ;
		
		long current = System.currentTimeMillis();
		long unixEpochStartTimeInNanos = TimeUnit.MILLISECONDS.toNanos(current + DELAY_TO_START);
		
		Instant	startInstant = Instant.parse(START_INSTANT);
		TestScenario testScenario = testScenario();
		
		// ---------------------------------------------------------------------
		// Creation phase
		// ---------------------------------------------------------------------
		
		// Broker creation
		int nbMaxChannelsSTD = 2;
		int nbMaxChannelsPREM = 4;
		String broker_URI = AbstractComponent.createComponent(
				Broker.class.getCanonicalName(),
				new Object[] {2, nbMaxChannelsSTD, nbMaxChannelsPREM, 1, 0});
		assert this.isDeployedComponent(broker_URI);
		this.toggleTracing(broker_URI);

		
		// Creation of 1 MeteoBureau
		
		String meteoBureau_URI_verif = AbstractComponent.createComponent(
				MeteoBureauTest.class.getCanonicalName(),
				new Object[] {MBRegistrationOutboundPortURI, MBPublishingOutboundPortURI, MBReceivingInboundPortURI,
						1, 1, bureauPosition, testScenario, MB_URI});
		assert MB_URI.equals(meteoBureau_URI_verif);
		assert this.isDeployedComponent(MB_URI);
		this.toggleTracing(MB_URI);
		
		// Creation of 2 MeteoStation
		
		String meteoStation1_URI_verif = AbstractComponent.createComponent(
				MeteoStationTest.class.getCanonicalName(),
				new Object[] {MS1RegistrationOutboundPortURI, MS1PublishingOutboundPortURI, MS1ReceivingInboundPortURI,
						1, 1, station1Position, testScenario, MS1_URI});
		assert MS1_URI.equals(meteoStation1_URI_verif);
		assert this.isDeployedComponent(MS1_URI);
		this.toggleTracing(MS1_URI);
	
		
		String meteoStation2_URI_verif = AbstractComponent.createComponent(
				MeteoStationTest.class.getCanonicalName(),
				new Object[] {MS2RegistrationOutboundPortURI, MS2PublishingOutboundPortURI, MS2ReceivingInboundPortURI,
						1, 1, station2Position, testScenario, MS2_URI});
		assert MS2_URI.equals(meteoStation2_URI_verif);
		assert this.isDeployedComponent(MS2_URI);
		this.toggleTracing(MS2_URI);
		
		// Creation of 1 WindTurbine between both MeteoStations
		
		String tmp_windTurbine_URI = AbstractComponent.createComponent(WindTurbine.class.getCanonicalName(),
				new Object[] {WindTurbineRegistrationOutboundPortURI,WindTurbinePublishingOutboundPortURI,WindTurbineReceivingInboundPortURI,
						1, 0, windTurbinePosition, maxWorkingLevelWT});
		this.toggleTracing(tmp_windTurbine_URI);
		
		// To uncomment if the windturbine participates to the scenario (in a step of it)
		/*String windTurbine_URI_verif = WT_URI = AbstractComponent.createComponent(WindTurbineTest.class.getCanonicalName(),
				new Object[] {WindTurbineRegistrationOutboundPortURI,WindTurbinePublishingOutboundPortURI,WindTurbineReceivingInboundPortURI,
						1, 1, windTurbinePosition, maxWorkingLevelWT, testScenario, WT_URI});
		assert WT_URI.equals(windTurbine_URI_verif);
		assert this.isDeployedComponent(WT_URI);
		this.toggleTracing(WT_URI);*/
		
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

		// --------------------------------------------------------------------
		// Deployment done
		// --------------------------------------------------------------------

		super.deploy();
		assert this.deploymentDone();
	}
	
	public static void main(String[] args) {
		try {
			VerboseException.VERBOSE = true;
			VerboseException.PRINT_STACK_TRACE = true;
			
			CVMTest cvm = new CVMTest() ;
			cvm.startStandardLifeCycle(EXECUTION_DURATION);
			Thread.sleep(END_SLEEP_DURATION);
			System.exit(0) ;
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
}
