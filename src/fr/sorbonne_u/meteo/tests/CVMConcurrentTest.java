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
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertyFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.TimeFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.exceptions.VerboseException;
import fr.sorbonne_u.filters.MessageFilter;
import fr.sorbonne_u.filters.property.PropertiesFilter;
import fr.sorbonne_u.filters.property.PropertyFilter;
import fr.sorbonne_u.filters.time.GreaterOrEqualTimeFilter;
import fr.sorbonne_u.filters.time.JokerTimeFilter;
import fr.sorbonne_u.filters.value.meteo.PositionInRegionsValueFilter;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.meteo.data.RegionData;
import fr.sorbonne_u.utils.aclocks.ClocksServer;

/**
 * Test class to simulate the following scenario from the start of the CVM.
 * 
 * <p>
 * 	SETUP
 * </p>
 * <ul>
 *     <li>The {@link Broker} has :
 *         <ul>
 *             <li>4 normal threads</li>
 *             <li>4 loading threads</li>
 *             <li>4 unloading threads</li>
 *             <li>2 FREE sending threads, 4 STANDARD sending threads, 8 PREMIUM sending threads</li>
 *         </ul>
 *     </li>
 *     <li>1 (FREE) {@link WindTurbineTest} accepting everything, with 4 threads</li>
 *     <li>3 FREE {@link MeteoStationTest}, each with 4 threads</li>
 *     <li>3 STANDARD {@link MeteoStationTest}, each with 4 threads</li>
 *     <li>3 PREMIUM {@link MeteoStationTest}, each with 4 threads</li>
 * </ul>
 * 
 * <ul>
 * 	<li> - After 20 seconds, <strong>all 3 FREE MeteoStations</strong> each send 30 wind datas to 'channel0'.
 * 	<li> - After 40 seconds, <strong>WindTurbine</strong> checks that it has received 90 wind datas.
 * 	<li> - After 60 seconds, <strong>all 3 STANDARD MeteoStations</strong> each send 30 wind datas to 'channel0'.
 * 	<li> - After 80 seconds, <strong>WindTurbine</strong> checks that it has received a total of 180 wind datas.
 * 	<li> - After 100 seconds, <strong>all 3 PREMIUM MeteoStations</strong> each send 30 wind datas to 'channel0'.
 * 	<li> - After 120 seconds, <strong>WindTurbine</strong> checks that it has received a total of 270 wind datas.
 * </ul>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class CVMConcurrentTest extends AbstractCVM {
	
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
	
	// Delay after the start when the step will be executed, in seconds
	public static long STEP0 = 20; // all 3 FREE MeteoStations each send 30 wind datas to 'channel0'
	public static long STEP1 = 40; // WindTurbine checks that it has received 90 wind datas.
	public static long STEP2 = 60; // all 3 STANDARD MeteoStations each send 30 wind datas to 'channel0'
	public static long STEP3 = 80; // WindTurbine checks that it has received 180 wind datas.
	public static long STEP4 = 100; // all 3 PREMIUM MeteoStations each send 30 wind datas to 'channel0'
	public static long STEP5 = 120; // WindTurbine checks that it has received 270 wind datas.

	// -------------------------------------------------------------------------
	// Component datas
	// -------------------------------------------------------------------------
	
	// Broker
	protected static final int brokerNbThreads = 4;
	protected static final int brokerNbLoadersThreads = 4;
	protected static final int brokerNbUnloadersThreads = 4;
	protected static final int brokerNbFreeSendersThreads = 2; // Broker's constructor automatically scales at 4 and 8 threads for Standard and Premium senders.
	
	// MeteoStation general URI
	protected final static String MS_URI = "MeteoStation-URI";
	protected final String MSReceivingInboundPortURI = "inbound-port-client-MS";
	protected final String MSRegistrationOutboundPortURI = "registration-outbound-port-MS";
	protected final String MSPublishingOutboundPortURI = "publishing-outbound-port-MS";
	protected final String MSPrivilegedOutboundPortURI = "privileged-outbound-port-MS";
	
	protected static final int stationNbThreads = 4;
	protected static final PositionData FREEstationPosition = new PositionData(1, 3);
	protected static final PositionData STANDARDstationPosition = new PositionData(2, 3);
	protected static final PositionData PREMIUMstationPosition = new PositionData(3, 3);
	
	// WindTurbine
	protected final static String WT_URI = "WindTurbine-URI";
	protected final String WindTurbineReceivingInboundPortURI = "inbound-port-client-WindTurbine";
	protected final String WindTurbineRegistrationOutboundPortURI = "registration-outbound-port-WindTurbine";
	protected final String WindTurbinePublishingOutboundPortURI = "publishing-outbound-port-WindTurbine";
	
	protected static final int windTurbineNbThreads = 4;
	protected static final PositionData windTurbinePosition = new PositionData(2, 2);
	
    protected TimeFilterI timeFilter = new JokerTimeFilter();
    protected MessageFilterI windTurbineWindFilter = new MessageFilter(new PropertyFilter[0], new PropertiesFilter[0], timeFilter);
    protected MessageFilterI windTurbineAlertFilter = new MessageFilter(new PropertyFilter[0], new PropertiesFilter[0], timeFilter);
	
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
		
		return new TestScenario(
			CLOCK_URI,
			startInstant,
			endInstant,
			new TestStepI[] {
				// MeteoStation1 sends wind data to 'channel0'.
				new TestStep(
					CLOCK_URI,
					MS_URI+1,
					step0_Instant,
					owner -> {
						try {
							System.out.println("STEP 0/5 (30s)");
							((MeteoStationTest)owner).publishWindData(2.0, 2.0, "channel0");
						} catch (Exception e) {
							System.err.println("Step 0 wind data publication failed with exception " + e);
						}
					})
				});
				//TODO
	}
	
	public CVMConcurrentTest() throws Exception{
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
				new Object[] {2, nbMaxChannelsSTD, nbMaxChannelsPREM, brokerNbThreads, 0, 
						brokerNbLoadersThreads, brokerNbUnloadersThreads, brokerNbFreeSendersThreads});
		assert this.isDeployedComponent(broker_URI);
		this.toggleTracing(broker_URI);
		
		// Creation of 9 MeteoStations
		for (int id=1; id<10; id++) {
			
			String privilegedOutboundPortURI = id > 3 ? MSPrivilegedOutboundPortURI : "";
			RegistrationClass rc = id <= 3 ? RegistrationClass.FREE : (id <= 6 ? RegistrationClass.STANDARD : RegistrationClass.PREMIUM);
			PositionData pos = id <= 3 ? FREEstationPosition : (id <= 6 ? STANDARDstationPosition : PREMIUMstationPosition);
			
			String meteoStation_URI_verif = AbstractComponent.createComponent(
					MeteoStationTest.class.getCanonicalName(),
					new Object[] { MSReceivingInboundPortURI+id, MSRegistrationOutboundPortURI+id, MSPublishingOutboundPortURI+id, privilegedOutboundPortURI+id,
							rc, stationNbThreads, 0, pos, MS_URI+id, testScenario, id});
			assert (MS_URI+id).equals(meteoStation_URI_verif);
			assert this.isDeployedComponent(MS_URI+id);
			this.toggleTracing(MS_URI+id);
		}
		
		// Creation of 1 WindTurbine
		String tmp_windTurbine_URI = AbstractComponent.createComponent(WindTurbineTest.class.getCanonicalName(),
				new Object[] { WindTurbineReceivingInboundPortURI, WindTurbineRegistrationOutboundPortURI, WindTurbinePublishingOutboundPortURI,
						RegistrationClass.FREE, 1, 1, windTurbinePosition, MeteoAlertI.Level.ORANGE, WT_URI, 1, windTurbineWindFilter, windTurbineAlertFilter});
		this.toggleTracing(tmp_windTurbine_URI);
		
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
			
			CVMConcurrentTest cvm = new CVMConcurrentTest() ;
			cvm.startStandardLifeCycle(EXECUTION_DURATION);
			Thread.sleep(END_SLEEP_DURATION);
			System.exit(0) ;
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
}
