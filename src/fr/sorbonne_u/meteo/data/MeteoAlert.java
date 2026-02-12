package fr.sorbonne_u.meteo.data;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;

import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI;
import fr.sorbonne_u.cps.meteo.interfaces.RegionI;

public class MeteoAlert implements MeteoAlertI {
	
	
	private static final long serialVersionUID = 1L;
	
	private AlertTypeI alertType;
	private LevelI alertLevel;
	private ArrayList<RegionI>regions;
	private Instant startTime;
	private Duration duration;
	
	
	
	public MeteoAlert(AlertTypeI alertType,LevelI alertLevel,ArrayList<RegionI> regions,Instant startTime) {
		this.alertType = alertType;
		this.alertLevel = alertLevel;
		this.regions = regions;
		this.startTime = startTime;
		
		this.duration = duration;
	}
	

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

}
