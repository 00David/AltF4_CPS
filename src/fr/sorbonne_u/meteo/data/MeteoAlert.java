package fr.sorbonne_u.meteo.data;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;

import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI;
import fr.sorbonne_u.cps.meteo.interfaces.RegionI;

/**
 * Meteo alerts emitted by meteo stations.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code alertType != null}
 * invariant	{@code alertLevel != null}
 * invariant	{@code regions != null && !regions.isEmpty()}
 * invariant	{@code startTime != null}
 * invariant	{@code duration != null}
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class MeteoAlert implements MeteoAlertI {
	
	private static final long serialVersionUID = -517623868931917800L;
	private AlertTypeI alertType;
	private LevelI alertLevel;
	private ArrayList<RegionI>regions;
	private Instant startTime;
	private Duration duration;
	
	/**
	 * Creates a meteo alert. 
	 * 
	 * <p>
	 * A least 1 region must be targeted and start time cannot be before now.
	 * </p>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code alertType != null}
	 * pre	{@code alertLevel != null}
	 * pre	{@code regions != null && !regions.isEmpty()}
	 * pre	{@code startTime != null}
	 * pre	{@code duration != null}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param alertType					The alert type.
	 * @param alertLevel				The alert level.
	 * @param regions					Regions targeted by the alert.
	 * @param startTime					The start time of the alert.
	 * @param duration					The duration time of the alert.
	 * @throws IllegalArgumentException	if a precondition is not satisfied.
	 */
	public MeteoAlert(AlertTypeI alertType,LevelI alertLevel,ArrayList<RegionI> regions,Instant startTime, Duration duration) {
		// Precondition checks
		if (alertType == null) throw new IllegalArgumentException("An alert type cannot be null");
		if (alertLevel == null) throw new IllegalArgumentException("An alert level cannot be null");
		if (regions == null || regions.isEmpty()) throw new IllegalArgumentException("Regions cannot be null or empty");
		if (startTime == null) throw new IllegalArgumentException("Start time cannot be null");
		if (duration == null) throw new IllegalArgumentException("Duration cannot be null");
		
		this.alertType = alertType;
		this.alertLevel = alertLevel;
		this.regions = regions;
		this.startTime = startTime;
		this.duration = duration;
	}
	
	// Getters postconditions are satisfied because of class invariants
	
	@Override
	public AlertTypeI getAlertType() {
		return this.alertType;
	}

	@Override
	public LevelI getLevel() {
		return this.alertLevel;
	}

	@Override
	public RegionI[] getRegions() {
		// .toArray(new RegionI[0]) : converts the Collection to a RegionI array
		return this.regions.toArray(new RegionI[0]);
	}

	@Override
	public Instant getStartTime() {
		return this.startTime;
	}

	@Override
	public Duration getDuration() {
		return this.duration;
	}
	
	@Override
	public String toString() {
		String res = "MeteoAlert\n";
		res += "Type : "+alertType+"\n";
		res += "Level : "+alertLevel+"\n";
		res += "Regions : \n";
		for (RegionI ri : regions) {
			RegionData r = (RegionData)ri;
			double[] rLimits = r.getLimits();
			res += "- X["+rLimits[0]+":"+rLimits[1]+"], Y["+rLimits[2]+":"+rLimits[3]+"]\n";
		}
		res += "Start : "+startTime+"\n";
		res += "Duration (s) : "+duration.getSeconds();
		return res;
	}

}
