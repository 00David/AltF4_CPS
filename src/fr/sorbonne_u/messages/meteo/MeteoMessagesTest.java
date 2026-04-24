package fr.sorbonne_u.messages.meteo;

import fr.sorbonne_u.cps.meteo.interfaces.RegionI;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI.AlertType;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI.Level;
import fr.sorbonne_u.meteo.data.MeteoAlert;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.meteo.data.RegionData;
import fr.sorbonne_u.meteo.data.WindData;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;

/**
 * Unit test for meteo application messages, including :
 * 
 * <ul>
 * 	<li> {@link AlertMessage} </li>
 * 	<li> {@link WindMessage} </li>
 * </ul>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class MeteoMessagesTest {

	// --------------------------------------------------------------------------
	// AlertMessage tests
	// --------------------------------------------------------------------------

	@Test
	public void testAlertMessageConstructorAndGetters() {
		ArrayList<RegionI> regions = new ArrayList<>();
		RegionData r1 = new RegionData(0, 10, 0, 10);
		RegionData r2 = new RegionData(2, 5, 6, 10);
		regions.add(r1); regions.add(r2);
		Instant now = Instant.now();
		Duration duration = Duration.ofHours(2);

		MeteoAlert alert = new MeteoAlert(AlertType.FLOODING, Level.RED, regions, now, duration);
		AlertMessage msg = new AlertMessage(alert);

		// Getter tests
		assertEquals(alert, msg.getPayload());
		assertEquals(AlertType.FLOODING, msg.getAlertType());
		assertEquals(Level.RED, msg.getLevel());
		assertEquals(regions.size(), msg.getRegions().size());
		assertEquals(r1, msg.getRegions().get(0));
		assertEquals(r2, msg.getRegions().get(1));
		assertEquals(now, msg.getStartTime());
		assertEquals(duration, msg.getDuration());
		
		// Invalid constructor argument
		assertThrows(IllegalArgumentException.class, () -> new AlertMessage(null));
	}

	@Test
	public void testAlertMessageRestrictedPutProperty() {
		ArrayList<RegionI> regions = new ArrayList<>();
		regions.add(new RegionData(0, 10, 0, 10));
		MeteoAlert alert = new MeteoAlert(AlertType.FLOODING, Level.RED, regions, Instant.now(), Duration.ofHours(1));
		AlertMessage msg = new AlertMessage(alert);

		// Replacing a meteo alert property value : KO
		assertThrows(IllegalArgumentException.class, () -> msg.putProperty(AlertMessage.ALERT_TYPE_PROPERTY, AlertType.ICY_STORM));

		// Replacing another property value : OK
		assertDoesNotThrow(() -> msg.putProperty("custom", "value"));
	}

	@Test
	public void testAlertMessageRestrictedRemoveProperty() throws Exception {
		ArrayList<RegionI> regions = new ArrayList<>();
		regions.add(new RegionData(0, 10, 0, 10));
		MeteoAlert alert = new MeteoAlert(AlertType.FLOODING, Level.RED, regions, Instant.now(), Duration.ofHours(1));
		AlertMessage msg = new AlertMessage(alert);

		// Removing a meteo alert property : KO
		assertThrows(IllegalArgumentException.class, () -> msg.removeProperty(AlertMessage.ALERT_LEVEL_PROPERTY));

		// Removing another property : OK
		msg.putProperty("custom", "value");
		assertDoesNotThrow(() -> msg.removeProperty("custom"));
	}

	@Test
	public void testAlertMessageSetPayloadForbidden() {
		ArrayList<RegionI> regions = new ArrayList<>();
		regions.add(new RegionData(0, 10, 0, 10));
		MeteoAlert alert = new MeteoAlert(AlertType.FLOODING, Level.RED, regions, Instant.now(), Duration.ofHours(1));
		AlertMessage msg = new AlertMessage(alert);

		// Replacing the payload : KO
		assertThrows(IllegalArgumentException.class, () -> msg.setPayload(alert));
	}

	// --------------------------------------------------------------------------
	// WindMessage tests
	// --------------------------------------------------------------------------

	@Test
	public void testWindMessageConstructorAndGetters() {
		PositionData pos = new PositionData(10, 20);
		WindData wind = new WindData(pos, 5.0, -3.0);

		WindMessage msg = new WindMessage(wind);

		// Getter tests
		assertNotNull(msg.getPayload());
		assertEquals(pos, msg.getPosition());
		assertEquals(5.0, msg.getX());
		assertEquals(-3.0, msg.getY());
		
		// Invalid constructor argument
		assertThrows(IllegalArgumentException.class, () -> new WindMessage(null));
	}

	@Test
	public void testWindMessageRestrictedPutProperty() {
		WindData wind = new WindData(new PositionData(0, 0), 1, 1);
		WindMessage msg = new WindMessage(wind);

		// Replacing a wind property value : KO
		assertThrows(IllegalArgumentException.class, () -> msg.putProperty(WindMessage.WIND_X_PROPERTY, 10.0));

		// Replacing another property value : OK
		assertDoesNotThrow(() -> msg.putProperty("custom", "value"));
	}

	@Test
	public void testWindMessageRestrictedRemoveProperty() throws Exception {
		WindData wind = new WindData(new PositionData(0, 0), 1, 1);
		WindMessage msg = new WindMessage(wind);

		// Removing a wind property : KO
		assertThrows(IllegalArgumentException.class, () -> msg.removeProperty(WindMessage.WIND_Y_PROPERTY));

		// Removing another property : OK
		msg.putProperty("custom", "value");
		assertDoesNotThrow(() -> msg.removeProperty("custom"));
	}

	@Test
	public void testWindMessageSetPayloadForbidden() {
		WindData wind = new WindData(new PositionData(0, 0), 1, 1);
		WindMessage msg = new WindMessage(wind);

		// Replacing the payload : KO
		assertThrows(IllegalArgumentException.class, () -> msg.setPayload(wind));
	}
}